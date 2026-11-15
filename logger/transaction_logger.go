package logger

import (
	"database/sql"
	"kv-store/event"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan event.Event, <-chan error)
	Run()
}

type PostgresTransactionLogger struct {
	events chan<- event.Event
	errors <-chan error
	db     *sql.DB
}

func (l *PostgresTransactionLogger) WritePut(key, value string) {
	l.events <- event.Event{EventType: event.EventPut, Key: key, Value: value}
}

func (l *PostgresTransactionLogger) WriteDelete(key string) {
	l.events <- event.Event{EventType: event.EventDelete, Key: key}
}

func (l *PostgresTransactionLogger) Err() <-chan error {
	return l.errors
}
