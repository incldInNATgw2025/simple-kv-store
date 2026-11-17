package logger

import "kv-store/event"

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	ReadEvents() (<-chan event.Event, <-chan error)
	Run()
	LastSequence() uint64
}
