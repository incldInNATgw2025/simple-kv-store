package service

import (
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/gorilla/mux"
)

const maxBody = 1 << 20 // phải khớp maxValueSize trong PutHandler

func resetStore() {
	store.Lock()
	store.m = make(map[string]string)
	store.Unlock()
}

func snapshot() map[string]string {
	store.RLock()
	defer store.RUnlock()
	cp := make(map[string]string, len(store.m))
	for k, v := range store.m {
		cp[k] = v
	}
	return cp
}

func newRouter() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/v1/key/{key}", PutHandler).Methods(http.MethodPut)
	r.HandleFunc("/v1/key/{key}", GetHandler).Methods(http.MethodGet)
	r.HandleFunc("/v1/key/{key}", DeleteHandler).Methods(http.MethodDelete)
	return r
}

func do(h http.Handler, method, key, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "/v1/key/"+key, strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func startService(t *testing.T) (http.Handler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "tx.log")
	resetStore()
	if err := InitializeTransactionLogAt(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = CloseTransactionLog() })
	return newRouter(), path
}

// restart mô phỏng tắt có trật tự rồi khởi động lại với store rỗng.
func restart(t *testing.T, path string) http.Handler {
	t.Helper()
	if err := CloseTransactionLog(); err != nil {
		t.Fatalf("close: %v", err)
	}
	resetStore()
	if err := InitializeTransactionLogAt(path); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	return newRouter()
}

func TestPutGetDelete(t *testing.T) {
	r, _ := startService(t)

	if rec := do(r, http.MethodPut, "a", "1"); rec.Code != http.StatusCreated {
		t.Fatalf("PUT: expected 201, got %d", rec.Code)
	}
	if rec := do(r, http.MethodGet, "a", ""); rec.Code != http.StatusOK || rec.Body.String() != "1" {
		t.Fatalf("GET: got %d %q", rec.Code, rec.Body.String())
	}
	if rec := do(r, http.MethodDelete, "a", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("DELETE: expected 204, got %d", rec.Code)
	}
	if rec := do(r, http.MethodGet, "a", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("GET after delete: expected 404, got %d", rec.Code)
	}
}

func TestPutOverwrites(t *testing.T) {
	r, _ := startService(t)
	do(r, http.MethodPut, "a", "1")
	do(r, http.MethodPut, "a", "2")
	if rec := do(r, http.MethodGet, "a", ""); rec.Body.String() != "2" {
		t.Fatalf("expected 2, got %q", rec.Body.String())
	}
}

func TestGetMissingKeyReturns404(t *testing.T) {
	r, _ := startService(t)
	if rec := do(r, http.MethodGet, "nope", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestDeleteMissingKeyIsIdempotent(t *testing.T) {
	r, _ := startService(t)
	if rec := do(r, http.MethodDelete, "nope", ""); rec.Code != http.StatusNoContent {
		t.Fatalf("expected 204, got %d", rec.Code)
	}
}

func TestPutExactlyMaxSizeIsAccepted(t *testing.T) {
	r, _ := startService(t)
	if rec := do(r, http.MethodPut, "big", strings.Repeat("x", maxBody)); rec.Code != http.StatusCreated {
		t.Fatalf("expected 201, got %d", rec.Code)
	}
}

func TestPutTooLargeReturns413AndIsNotLogged(t *testing.T) {
	r, path := startService(t)

	rec := do(r, http.MethodPut, "big", strings.Repeat("x", maxBody+1))
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413, got %d", rec.Code)
	}
	if _, err := Get("big"); !errors.Is(err, ErrorNoSuchKey) {
		t.Fatalf("store must not change, got %v", err)
	}
	if err := CloseTransactionLog(); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("rejected request must not be logged, log has %d bytes", len(data))
	}
}

// Logger đã đóng: handler trả 500 và store không bị thay đổi.
func TestHandlersFailWhenLoggerClosed(t *testing.T) {
	r, _ := startService(t)
	do(r, http.MethodPut, "keep", "1")

	if err := CloseTransactionLog(); err != nil {
		t.Fatal(err)
	}

	if rec := do(r, http.MethodPut, "new", "x"); rec.Code != http.StatusInternalServerError {
		t.Fatalf("PUT: expected 500, got %d", rec.Code)
	}
	if _, err := Get("new"); !errors.Is(err, ErrorNoSuchKey) {
		t.Fatalf("failed PUT must not change the store, got %v", err)
	}
	if rec := do(r, http.MethodDelete, "keep", ""); rec.Code != http.StatusInternalServerError {
		t.Fatalf("DELETE: expected 500, got %d", rec.Code)
	}
	if v, err := Get("keep"); err != nil || v != "1" {
		t.Fatalf("failed DELETE must not change the store, got %q, %v", v, err)
	}
}

func TestRestartRestoresState(t *testing.T) {
	r, path := startService(t)
	do(r, http.MethodPut, "a", "1")
	do(r, http.MethodPut, "b", "2")
	do(r, http.MethodPut, "a", "3") // ghi đè
	do(r, http.MethodPut, "c", "hello world")
	do(r, http.MethodDelete, "b", "")

	r = restart(t, path)

	if rec := do(r, http.MethodGet, "a", ""); rec.Body.String() != "3" {
		t.Fatalf("a: got %q, want 3", rec.Body.String())
	}
	if rec := do(r, http.MethodGet, "b", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("b should stay deleted, got %d", rec.Code)
	}
	if rec := do(r, http.MethodGet, "c", ""); rec.Body.String() != "hello world" {
		t.Fatalf("c: got %q", rec.Body.String())
	}
}

func TestRestartKeepsSpecialValues(t *testing.T) {
	values := map[string]string{
		"newline": "dong1\ndong2",
		"tab":     "a\tb",
		"empty":   "",
		"quote":   `say "hi"`,
		"unicode": "giữ lại ✓",
		"binary":  "\xff\x00\x01",
	}
	r, path := startService(t)
	for k, v := range values {
		if rec := do(r, http.MethodPut, k, v); rec.Code != http.StatusCreated {
			t.Fatalf("PUT %s: got %d", k, rec.Code)
		}
	}

	restart(t, path)

	for k, want := range values {
		if got, err := Get(k); err != nil || got != want {
			t.Fatalf("%s: got %q, %v; want %q", k, got, err, want)
		}
	}
}

// Value lớn: dòng log có thể dài hơn 64 KB, replay vẫn phải đọc được.
func TestRestartKeepsLargeValues(t *testing.T) {
	values := map[string]string{
		"text": strings.Repeat("x", maxBody),
		"bin":  strings.Repeat("\xff", maxBody), // %q nở mỗi byte thành 4 ký tự
	}
	r, path := startService(t)
	for k, v := range values {
		if rec := do(r, http.MethodPut, k, v); rec.Code != http.StatusCreated {
			t.Fatalf("PUT %s: got %d", k, rec.Code)
		}
	}

	restart(t, path)

	for k, want := range values {
		if got, err := Get(k); err != nil || got != want {
			t.Fatalf("%s: replayed value differs (len got=%d want=%d, err=%v)", k, len(got), len(want), err)
		}
	}
}

// Nhiều lần khởi động liên tiếp: nếu số thứ tự quay về 1,
// lần replay sau sẽ báo "out of sequence".
func TestMultipleRestartsKeepAppending(t *testing.T) {
	r, path := startService(t)
	do(r, http.MethodPut, "k1", "v1")

	r = restart(t, path)
	do(r, http.MethodPut, "k2", "v2")

	r = restart(t, path)
	do(r, http.MethodPut, "k3", "v3")

	restart(t, path)

	for k, want := range map[string]string{"k1": "v1", "k2": "v2", "k3": "v3"} {
		if got, err := Get(k); err != nil || got != want {
			t.Fatalf("%s: got %q, %v; want %q", k, got, err, want)
		}
	}
}

func TestInitializeFailsOnCorruptLog(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tx.log")
	if err := os.WriteFile(path, []byte("not a valid line\n"), 0644); err != nil {
		t.Fatal(err)
	}
	resetStore()
	if err := InitializeTransactionLogAt(path); err == nil {
		t.Fatal("expected an error for a corrupt log")
	}
}

// Kiểm tra thứ tự: sau khi nhiều request ghi/xóa song song, replay log
// phải cho ra đúng trạng thái của store lúc đó (bắt lỗi nếu bỏ writeMu).
// Chạy với -race và -count lớn vì lỗi thứ tự chỉ xuất hiện theo xác suất.
func TestConcurrentOpsReplayMatchesStore(t *testing.T) {
	r, path := startService(t)

	const workers, ops, keys = 8, 300, 5
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w)))
			for i := 0; i < ops; i++ {
				key := fmt.Sprintf("k%d", rng.Intn(keys))
				switch rng.Intn(3) {
				case 0:
					if rec := do(r, http.MethodDelete, key, ""); rec.Code != http.StatusNoContent {
						t.Errorf("DELETE %s: got %d", key, rec.Code)
					}
				case 1:
					do(r, http.MethodGet, key, "") // đọc song song với ghi
				default:
					if rec := do(r, http.MethodPut, key, fmt.Sprintf("w%d-%d", w, i)); rec.Code != http.StatusCreated {
						t.Errorf("PUT %s: got %d", key, rec.Code)
					}
				}
			}
		}(w)
	}
	wg.Wait()

	want := snapshot()
	restart(t, path)
	got := snapshot()

	if !reflect.DeepEqual(got, want) {
		t.Fatalf("state after replay differs from live state:\n live:   %v\n replay: %v", want, got)
	}
}
