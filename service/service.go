package service

import (
	"errors"
	"fmt"
	"io"
	"kv-store/event"
	transactionlog "kv-store/transaction_log"
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

var writeMu sync.Mutex

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

	const maxValueSize = 1 << 20 // 1 MiB

	r.Body = http.MaxBytesReader(w, r.Body, maxValueSize)
	value, err := io.ReadAll(r.Body)
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			http.Error(w, "value too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer r.Body.Close()

	writeMu.Lock()
	err = logger.WritePut(key, string(value))
	if err == nil {
		err = Put(key, string(value))
	}
	writeMu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

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
}

func DeleteHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	writeMu.Lock()
	err := logger.WriteDelete(key)

	if err == nil {
		err = Delete(key)
	}
	writeMu.Unlock()

	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

var logger transactionlog.TransactionLogger

func InitializeTransactionLog() error {
	return InitializeTransactionLogAt("transaction.log")
}

func InitializeTransactionLogAt(path string) error {
	l, err := transactionlog.NewFileTransactionLogger(path)
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	logger = l

	events, errCh := l.ReadEvents()
	for events != nil || errCh != nil {
		select {
		case err, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			_ = l.Close()
			return fmt.Errorf("failed to replay transaction log: %w", err)

		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			switch e.EventType {
			case event.EventDelete:
				_ = Delete(e.Key)
			case event.EventPut:
				_ = Put(e.Key, e.Value)
			}
		}
	}

	l.Run()
	go func() {
		for err := range l.Err() {
			log.Printf("transaction log write failed: %s", err)
		}
	}()
	return nil
}

func CloseTransactionLog() error {
	return logger.Close()
}
