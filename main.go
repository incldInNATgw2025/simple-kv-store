package main

import (
	"errors"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

var store = make(map[string]string)

func Put(key, value string) error {
	store[key] = value

	return nil
}

var ErrorNoSuchKey = errors.New("no such key")

func Get(key string) (string, error) {
	value, ok := store[key]

	if !ok {
		return "", ErrorNoSuchKey
	}
	return value, nil
}

func Delete(key string) error {
	delete(store, key)
	return nil
}

func helloGoHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello net/http!")
}

func helloMuxHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintln(w, "Hello gorilla/mux!")
}

func main() {
	r := mux.NewRouter()
	r.HandleFunc("/", helloMuxHandler)

	log.Fatal(http.ListenAndServe(":8082", r))

}
