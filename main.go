package main

import (
	"kv-store/service"
	"log"
	"net/http"

	"github.com/gorilla/mux"
)

func main() {

	err := service.InitializeTransactionLog()
	if err != nil {
		log.Fatalf("failed to start transaction log: %s", err.Error())
		return
	}

	r := mux.NewRouter()
	r.HandleFunc("/v1/key/{key}", service.PutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/key/{key}", service.GetHandler).Methods(http.MethodGet)

	log.Fatal(http.ListenAndServe(":8082", r))

	err = service.CloseTransactionLog()
	if err != nil {
		log.Fatalf("failed to close transaction log file: %s", err.Error())
	}
}
