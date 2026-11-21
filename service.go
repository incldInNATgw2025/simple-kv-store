package main

import (
	"errors"
	"fmt"
	"io/ioutil"
	"kv-store/core"
	"kv-store/event"
	"kv-store/logger"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var transact logger.TransactionLogger

func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		log.Println(r.Method, r.RequestURI)
		next.ServeHTTP(w, r)
	})
}

func notAllowedHandler(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "Not Allowed", http.StatusMethodNotAllowed)
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	err = core.Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	transact.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)

	log.Printf("PUT key%s value=%s\n", key, string(value))
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := core.Get(key)
	if errors.Is(err, core.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(value))

	log.Printf("GET key=%s\n", key)
}

func keyValueDeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := core.Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	transact.WriteDelete(key)

	log.Printf("DELETE key=%s\n", key)
}

func initializeTransactionLog() error {
	var err error
	transact, err = logger.NewPostgresTransactionLogger(logger.PostgresDBParams{
		Host:     "localhost",
		DbName:   "kvs",
		User:     "test",
		Password: "hunter2",
	})
	if err != nil {
		return fmt.Errorf("failed to create transaction logger: %w", err)
	}

	events, errors := transact.ReadEvents()
	count, ok, e := 0, true, event.Event{}

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case event.EventDelete:
				err = core.Delete(e.Key)
				count++
			case event.EventPut:
				err = core.Put(e.Key, e.Value)
				count++
			}
		}
	}
	log.Printf("%d events replayed\n", count)
	transact.Run()

	go func() {
		for err := range transact.Err() {
			log.Print(err)
		}
	}()

	return err
}

func main() {
	// Initializes the transaction log and loads existing data, if any.
	// Blocks until all data is read.
	err := initializeTransactionLog()
	if err != nil {
		panic(err)
	}

	// Create a new mux router
	r := mux.NewRouter()

	r.Use(loggingMiddleware)

	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueDeleteHandler).Methods("DELETE")

	r.HandleFunc("/v1", notAllowedHandler)
	r.HandleFunc("/v1/{key}", notAllowedHandler)

	log.Fatal(http.ListenAndServeTLS(":8082", "public.pem", "private.pem", r))
}
