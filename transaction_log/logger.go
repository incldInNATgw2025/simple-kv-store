package transactionlog

import (
	"bufio"
	"errors"
	"fmt"
	"kv-store/event"
	"os"
	"strconv"
	"strings"
	"sync"
)

var ErrLoggerClosed = errors.New("transaction logger is closed")

type TransactionLogger interface {
	WriteDelete(key string) error
	WritePut(key, value string) error
	Err() <-chan error
	Run()
	ReadEvents() (<-chan event.Event, <-chan error)
	Close() error
}

type FileTransactionLogger struct {
	events       chan<- event.Event
	errors       chan error
	lastSequence uint64
	file         *os.File
	wg           sync.WaitGroup
	w            *bufio.Writer

	mu     sync.RWMutex // bảo vệ closed và việc gửi vào channel
	closed bool

	errMu   sync.Mutex // bảo vệ failErr
	failErr error
}

func (l *FileTransactionLogger) setFailure(err error) {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	if l.failErr == nil { // giữ lỗi đầu tiên
		l.failErr = err
	}
}

func (l *FileTransactionLogger) failure() error {
	l.errMu.Lock()
	defer l.errMu.Unlock()
	return l.failErr
}

func (l *FileTransactionLogger) write(e event.Event) error {

	l.mu.RLock()
	defer l.mu.RUnlock()

	if l.closed {
		return ErrLoggerClosed
	}
	if l.events == nil {
		return errors.New("transaction logger is not running")
	}
	if err := l.failure(); err != nil {
		return fmt.Errorf("transaction log failed: %w", err)
	}
	l.events <- e
	return nil
}

func (l *FileTransactionLogger) WritePut(key, value string) error {
	return l.write(event.Event{EventType: event.EventPut, Key: key, Value: value})
}

func (l *FileTransactionLogger) WriteDelete(key string) error {
	return l.write(event.Event{EventType: event.EventDelete, Key: key})
}

func (l *FileTransactionLogger) Err() <-chan error {
	return l.errors
}

func NewFileTransactionLogger(filename string) (TransactionLogger, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("cannot open transaction log file: %w", err)
	}
	return &FileTransactionLogger{file: file}, nil
}

func (l *FileTransactionLogger) Close() error {

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrLoggerClosed
	}
	l.closed = true

	if l.events == nil { // Run chưa được gọi, chỉ cần đóng file
		l.mu.Unlock()
		return l.file.Close()
	}
	close(l.events) // không nhận thêm event
	l.mu.Unlock()
	l.wg.Wait() // goroutine đã rút hết channel và thoát

	var firstErr error
	keep := func(err error) {
		if err != nil && firstErr == nil {
			firstErr = err
		}
	}

	keep(l.failure())
	keep(l.w.Flush())    // 1. đẩy bufio xuống file
	keep(l.file.Sync())  // 2. đẩy từ bộ đệm HĐH xuống đĩa
	keep(l.file.Close()) // 3. luôn đóng file, kể cả khi bước trên lỗi
	close(l.errors)

	return firstErr
}

func (l *FileTransactionLogger) Run() {
	events := make(chan event.Event, 16)
	errCh := make(chan error, 1)

	l.mu.Lock()
	l.events = events
	l.errors = errCh
	l.w = bufio.NewWriter(l.file)
	l.mu.Unlock()

	l.wg.Add(1)
	//goroutine chạy riêng
	go func() {
		defer l.wg.Done()
		failed := false

		report := func(err error) {
			l.setFailure(err)
			failed = true
			select {
			case errCh <- err:
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
				"%d\t%d\t%q\t%q\n",
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
	scanner.Buffer(make([]byte, 0, 64*1024), 16*1024*1024)
	outEvent := make(chan event.Event)
	outError := make(chan error, 1)

	go func() {

		defer close(outEvent)
		defer close(outError)

		for scanner.Scan() {
			line := scanner.Text()
			parts := strings.SplitN(line, "\t", 4)
			if len(parts) != 4 {
				outError <- fmt.Errorf("input parse error: malformed line %q", line)
				return
			}

			seq, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			typ, err := strconv.ParseUint(parts[1], 10, 8)
			if err != nil {
				outError <- fmt.Errorf("input parse error: %w", err)
				return
			}
			if l.lastSequence >= seq {
				outError <- fmt.Errorf("transaction numbers out of sequence")
				return
			}
			l.lastSequence = seq
			key, err := strconv.Unquote(parts[2])
			if err != nil {
				outError <- fmt.Errorf("input parse error: bad key: %w", err)
				return
			}
			value, err := strconv.Unquote(parts[3])
			if err != nil {
				outError <- fmt.Errorf("input parse error: bad value: %w", err)
				return
			}
			outEvent <- event.Event{
				Sequence:  seq,
				EventType: event.EventType(typ), // đổi tên kiểu nếu package event đặt khác
				Key:       key,
				Value:     value,
			}

		}
		if err := scanner.Err(); err != nil {
			outError <- fmt.Errorf("transaction log read failure: %w", err)
			return
		}
	}()

	return outEvent, outError
}
