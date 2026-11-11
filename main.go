package main

import (
	"context"
	"errors"
	"kv-store/service"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

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

	srv := &http.Server{
		Addr:    ":8082",
		Handler: r,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			log.Printf("server error: %s", err.Error())
		}
	case <-ctx.Done():
		log.Println("shutting down...")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %s", err)
	}

	if err = service.CloseTransactionLog(); err != nil {
		log.Fatalf("failed to close transaction log file: %s", err.Error())
	}
}
