package service

import (
	"errors"
	"fmt"
	"io"
	"kv-store/event"
	transactionlog "kv-store/transaction_log"
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
	store.Lock()
	defer store.Unlock()
	delete(store.m, key)
	return nil
}

func PutHandler(w http.ResponseWriter, r *http.Request) {
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
	logger.WritePut(key, string(value))
	w.WriteHeader(http.StatusCreated)
}

func GetHandler(w http.ResponseWriter, r *http.Request) {
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
	w.WriteHeader(http.StatusOK)
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	err := Delete(key)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	logger.WriteDelete(key)
	w.WriteHeader(http.StatusOK)
}

var logger transactionlog.TransactionLogger

func InitializeTransactionLog() error {
	var err error

	logger, err = transactionlog.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errors := logger.ReadEvents()

	e := event.Event{}
	ok := true

	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case event.EventDelete:
				err = Delete(e.Key)
			case event.EvenPut:
				err = Put(e.Key, e.Value)
			}
		}
	}
	logger.Run()
	return err
}

func CloseTransactionLog() error {
	return logger.Close()
}
