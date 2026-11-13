package main

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

const base = "http://localhost:8082/v1/key/"

func portBusy() bool {
	c, err := net.DialTimeout("tcp", "localhost:8082", 200*time.Millisecond)
	if err != nil {
		return false
	}
	c.Close()
	return true
}

func startServer(t *testing.T, bin, dir string) *exec.Cmd {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Dir = dir // transaction.log sẽ nằm trong thư mục tạm này
	cmd.Stdout, cmd.Stderr = io.Discard, io.Discard
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(base + "probe"); err == nil {
			resp.Body.Close()
			return cmd
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("server did not become ready")
	return nil
}

func stopServer(t *testing.T, cmd *exec.Cmd) {
	t.Helper()
	if err := cmd.Process.Signal(os.Interrupt); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("server did not exit cleanly: %v", err)
		}
	case <-time.After(10 * time.Second):
		_ = cmd.Process.Kill()
		t.Fatal("server did not exit after SIGINT")
	}
}

func request(t *testing.T, method, key, body string) (int, string) {
	t.Helper()
	req, err := http.NewRequest(method, base+key, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestGracefulShutdownPersistsData(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}
	if runtime.GOOS == "windows" {
		t.Skip("needs SIGINT")
	}
	if portBusy() {
		t.Skip("port 8082 is already in use")
	}

	bin := filepath.Join(t.TempDir(), "kv")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build failed: %v\n%s", err, out)
	}
	dir := t.TempDir()

	// Phiên 1
	cmd := startServer(t, bin, dir)
	if code, _ := request(t, http.MethodPut, "a", "hello world"); code != http.StatusCreated {
		t.Fatalf("PUT a: got %d", code)
	}
	request(t, http.MethodPut, "b", "temp")
	if code, _ := request(t, http.MethodDelete, "b", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE b: got %d", code)
	}
	for i := 0; i < 500; i++ { // ghi nhanh để còn event trong buffer lúc tắt
		request(t, http.MethodPut, fmt.Sprintf("k%d", i), "v")
	}
	stopServer(t, cmd)

	// Phiên 2: phải khôi phục đủ dữ liệu, kể cả event ghi cuối cùng
	cmd = startServer(t, bin, dir)
	if code, body := request(t, http.MethodGet, "a", ""); code != http.StatusOK || body != "hello world" {
		t.Fatalf("a after restart: %d %q", code, body)
	}
	if code, _ := request(t, http.MethodGet, "b", ""); code != http.StatusNotFound {
		t.Fatalf("b should stay deleted, got %d", code)
	}
	if code, body := request(t, http.MethodGet, "k499", ""); code != http.StatusOK || body != "v" {
		t.Fatalf("last write was lost on shutdown: %d %q", code, body)
	}
	stopServer(t, cmd)
}
