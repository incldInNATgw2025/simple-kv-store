package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os/signal"
	"syscall"
	"time"

	"github.com/gorilla/mux"

	"kv-store/service"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if err := service.InitializeTransactionLog(); err != nil {
		return fmt.Errorf("failed to start transaction log: %w", err)
	}

	r := mux.NewRouter()
	r.HandleFunc("/v1/key/{key}", service.PutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/key/{key}", service.GetHandler).Methods(http.MethodGet)
	r.HandleFunc("/v1/key/{key}", service.DeleteHandler).Methods(http.MethodDelete)

	srv := &http.Server{
		Addr:              ":8082",
		Handler:           r,
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- srv.ListenAndServe()
	}()

	var runErr error
	select {
	case err := <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			runErr = fmt.Errorf("server error: %w", err)
		}
	case <-ctx.Done():
		log.Println("shutting down...")
		stop() // Ctrl+C lần hai sẽ tắt ngay
	}

	// 1. Ngừng nhận request mới, chờ request đang chạy xong
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("server shutdown error: %s", err)
	}

	// 2. Sau khi không còn request nào, mới flush và đóng log
	if err := service.CloseTransactionLog(); err != nil {
		log.Printf("failed to close transaction log file: %s", err)
		if runErr == nil {
			runErr = err
		}
	}
	return runErr
}
