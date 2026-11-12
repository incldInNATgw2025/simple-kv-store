package transactionlog

import (
	"bufio"
	"fmt"
	"kv-store/event"
	"os"
	"sync"
)

type TransactionLogger interface {
	WriteDelete(key string)
	WritePut(key, value string)
	Err() <-chan error
	Run()
	ReadEvents() (<-chan event.Event, <-chan error)
	Close() error
}

type FileTransactionLogger struct {
	events       chan<- event.Event
	errors       <-chan error
	lastSequence uint64
	file         *os.File
	wg           sync.WaitGroup
	w            *bufio.Writer
}

func (l *FileTransactionLogger) WritePut(key, value string) {
	l.events <- event.Event{EventType: event.EventPut, Key: key, Value: value}
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

func (l *FileTransactionLogger) Close() error {
	close(l.events) // không nhận thêm event
	l.wg.Wait()     // goroutine đã rút hết channel và thoát

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// lấy lỗi ghi trước đó (nếu có) từ goroutine
	select {
	case err := <-l.errors:
		keep(err)
	default:
	}

	keep(l.w.Flush())    // 1. đẩy bufio xuống file
	keep(l.file.Sync())  // 2. đẩy từ bộ đệm HĐH xuống đĩa
	keep(l.file.Close()) // 3. luôn đóng file, kể cả khi bước trên lỗi

	return firstErr
}

func (l *FileTransactionLogger) Run() {
	events := make(chan event.Event, 16)
	l.events = events

	errors := make(chan error, 1)
	l.errors = errors

	l.w = bufio.NewWriter(l.file)

	l.wg.Add(1)
	//goroutine chạy riêng
	go func() {
		defer l.wg.Done()
		failed := false

		report := func(err error) {
			failed = true
			select {
			case errors <- err:
			default:
			}
		}

		for e := range events {
			if failed {
				continue
			}
			l.lastSequence++

			_, err := fmt.Fprintf(
				l.w,
				"%d\t%d\t%s\t%s\n",
				l.lastSequence, e.EventType, e.Key, e.Value)
			if err != nil {
				report(err)
				continue
			}

			if len(events) == 0 {
				if err := l.w.Flush(); err != nil {
					report(err)
				}
			}
		}

		if !failed {
			if err := l.w.Flush(); err != nil {
				report(err)
			}
		}
	}()

}

func (l *FileTransactionLogger) ReadEvents() (<-chan event.Event, <-chan error) {
	scanner := bufio.NewScanner(l.file)
	outEvent := make(chan event.Event)
	outError := make(chan error, 1)

	go func() {
		var e event.Event

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()

			if _, err := fmt.Sscanf(line, "%d\t%d\t%s\t%s", &e.Sequence, &e.EventType, &e.Key, &e.Value); err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			if l.lastSequence >= e.Sequence {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}
			l.lastSequence = e.Sequence
			outEvent <- e

		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}
