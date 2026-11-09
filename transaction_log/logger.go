package transactionlog

import (
	"fmt"
	"kv-store/event"
	"os"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
}

type FileTransactionLogger struct {
	events       chan<- event.Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- event.Event{EventType: event.EvenPut, Key: key, Value: value}
}

func (l *FileTransactionLogger) WriteDelete(key string) {
	l.events <- event.Event{EventType: event.EventDelete, Key: key}
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0755)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}
