package transactionlog

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"kv-store/event"
)

func startLogger(t *testing.T) (TransactionLogger, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tx.log")
	l, err := NewFileTransactionLogger(path)
	if err != nil {
		t.Fatalf("NewFileTransactionLogger: %v", err)
	}
	l.Run()
	t.Cleanup(func() { _ = l.Close() }) // lỗi "đã đóng" khi test tự Close là bình thường
	return l, path
}

func reopen(t *testing.T, path string) TransactionLogger {
	t.Helper()
	l, err := NewFileTransactionLogger(path)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func countLines(t *testing.T, path string) int {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return bytes.Count(data, []byte("\n"))
}

// readAll đọc hết log, trả về các event và lỗi đầu tiên (nếu có).
func readAll(l TransactionLogger) ([]event.Event, error) {
	events, errs := l.ReadEvents()
	var got []event.Event
	var firstErr error
	timeout := time.After(10 * time.Second)

	for events != nil || errs != nil {
		select {
		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			got = append(got, e)
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if firstErr == nil {
				firstErr = err
			}
		case <-timeout:
			return got, errors.New("timeout reading events")
		}
	}
	return got, firstErr
}

// Bắt lỗi thiếu Flush khi đóng.
func TestCloseFlushesAllEvents(t *testing.T) {
	l, path := startLogger(t)

	const n = 1000
	for i := 0; i < n; i++ {
		if err := l.WritePut(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatalf("WritePut: %v", err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := countLines(t, path); got != n {
		t.Fatalf("expected %d lines, got %d", n, got)
	}
}

func TestRoundTrip(t *testing.T) {
	l, path := startLogger(t)
	check := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	check(l.WritePut("a", "1"))
	check(l.WritePut("b", "hello world"))
	check(l.WritePut("c", "line1\nline2\ttab \"quoted\""))
	check(l.WriteDelete("a"))
	check(l.Close())

	l2 := reopen(t, path)
	defer l2.Close()
	got, err := readAll(l2)
	if err != nil {
		t.Fatalf("readAll: %v", err)
	}

	want := []event.Event{
		{Sequence: 1, EventType: event.EventPut, Key: "a", Value: "1"},
		{Sequence: 2, EventType: event.EventPut, Key: "b", Value: "hello world"},
		{Sequence: 3, EventType: event.EventPut, Key: "c", Value: "line1\nline2\ttab \"quoted\""},
		{Sequence: 4, EventType: event.EventDelete, Key: "a", Value: ""},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("events differ:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestEmptyLogHasNoEvents(t *testing.T) {
	l := reopen(t, filepath.Join(t.TempDir(), "tx.log"))
	defer l.Close()
	got, err := readAll(l)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %d events, err=%v", len(got), err)
	}
}

// Sau khi khởi động lại, số thứ tự phải tiếp tục chứ không quay về 1.
func TestSequenceContinuesAfterReopen(t *testing.T) {
	l, path := startLogger(t)
	for i := 0; i < 3; i++ {
		if err := l.WritePut(fmt.Sprintf("k%d", i), "v"); err != nil {
			t.Fatal(err)
		}
	}
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}

	l2 := reopen(t, path)
	if _, err := readAll(l2); err != nil { // nạp lastSequence
		t.Fatal(err)
	}
	l2.Run()
	_ = l2.WritePut("x", "1")
	_ = l2.WritePut("y", "2")
	if err := l2.Close(); err != nil {
		t.Fatal(err)
	}

	l3 := reopen(t, path)
	defer l3.Close()
	got, err := readAll(l3)
	if err != nil {
		t.Fatalf("log after restart is unreadable: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("expected 5 events, got %d", len(got))
	}
	for i, e := range got {
		if e.Sequence != uint64(i+1) {
			t.Fatalf("event %d has sequence %d", i, e.Sequence)
		}
	}
}

func TestReadEventsRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"malformed line":      "garbage\n",
		"unquoted key":        "1\t1\tkey\t\"v\"\n",
		"unquoted value":      "1\t1\t\"k\"\tvalue\n",
		"non numeric seq":     "x\t1\t\"k\"\t\"v\"\n",
		"out of sequence":     "2\t1\t\"a\"\t\"1\"\n1\t1\t\"b\"\t\"2\"\n",
		"duplicate sequence":  "1\t1\t\"a\"\t\"1\"\n1\t1\t\"b\"\t\"2\"\n",
		"truncated last line": "1\t1\t\"a\"\t\"1\"\n2\t1\t\"b\"\t\"2",
	}
	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "tx.log")
			if err := os.WriteFile(path, []byte(content), 0644); err != nil {
				t.Fatal(err)
			}
			l := reopen(t, path)
			defer l.Close()
			if _, err := readAll(l); err == nil {
				t.Fatal("expected a parse error, got nil")
			}
		})
	}
}

func TestWriteAfterCloseReturnsErrLoggerClosed(t *testing.T) {
	l, _ := startLogger(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	// không được panic "send on closed channel" hay treo
	if err := l.WritePut("k", "v"); !errors.Is(err, ErrLoggerClosed) {
		t.Fatalf("WritePut: expected ErrLoggerClosed, got %v", err)
	}
	if err := l.WriteDelete("k"); !errors.Is(err, ErrLoggerClosed) {
		t.Fatalf("WriteDelete: expected ErrLoggerClosed, got %v", err)
	}
}

func TestCloseTwice(t *testing.T) {
	l, _ := startLogger(t)
	if err := l.Close(); err != nil {
		t.Fatal(err)
	}
	if err := l.Close(); !errors.Is(err, ErrLoggerClosed) {
		t.Fatalf("second Close: expected ErrLoggerClosed, got %v", err)
	}
}

func TestCloseAndWriteWithoutRun(t *testing.T) {
	l := reopen(t, filepath.Join(t.TempDir(), "tx.log"))
	if err := l.WritePut("k", "v"); err == nil {
		t.Fatal("expected an error when the logger is not running")
	}
	if err := l.Close(); err != nil {
		t.Fatalf("Close without Run: %v", err)
	}
}

// Khi Close chạy song song với nhiều writer: không panic, và
// mọi write trả nil đều phải nằm trong file.
func TestEveryAcceptedWriteIsPersisted(t *testing.T) {
	l, path := startLogger(t)

	var accepted int64
	var wg sync.WaitGroup
	for w := 0; w < 20; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; ; i++ {
				err := l.WritePut(fmt.Sprintf("w%d-%d", w, i), "v")
				if err != nil {
					if !errors.Is(err, ErrLoggerClosed) {
						t.Errorf("unexpected error: %v", err)
					}
					return
				}
				atomic.AddInt64(&accepted, 1)
			}
		}(w)
	}

	time.Sleep(30 * time.Millisecond)
	if err := l.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	wg.Wait()

	got, want := int64(countLines(t, path)), atomic.LoadInt64(&accepted)
	if got != want {
		t.Fatalf("accepted %d writes but file has %d lines", want, got)
	}
}

// Gây lỗi ghi bằng cách đóng file bên dưới.
func TestWriteFailureIsReported(t *testing.T) {
	l, _ := startLogger(t)

	fl := l.(*FileTransactionLogger)
	if err := fl.file.Close(); err != nil {
		t.Fatal(err)
	}
	_ = l.WritePut("k", "v") // goroutine ghi sẽ gặp lỗi khi flush

	select {
	case err := <-l.Err():
		if err == nil {
			t.Fatal("got nil error from Err()")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("write failure was not reported on Err()")
	}

	// Sau khi hỏng, các lần ghi sau phải bị từ chối thay vì báo thành công
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if err := l.WritePut("k2", "v"); err != nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("WritePut kept succeeding after the log failed")
}
