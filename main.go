package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

type LockableMap struct {
	sync.RWMutex
	m map[string]string
}

var store = LockableMap{
	m: make(map[string]string),
}

func Put(key, value string) error {
	store.Lock()
	defer store.Unlock()
	store.m[key] = value

	return nil
}

var ErrorNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	store.RLock()
	defer store.RUnlock()
	value, ok := store.m[key]

	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func Delete(key string) error {
	store.RLock()
	defer store.RUnlock()
	delete(store.m, key)
	return nil
}

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	err = Put(key, string(value))
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusCreated)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	fmt.Fprint(w, value)
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/v1/key/{key}", putHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/key/{key}", getHandler).Methods(http.MethodGet)

	log.Fatal(http.ListenAndServe(":8082", r))

}
