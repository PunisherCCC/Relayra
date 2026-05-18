package server

import (
	"bufio"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
)

type hijackableRecorder struct {
	*httptest.ResponseRecorder
	hijacked bool
	flushed  bool
}

func (r *hijackableRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	r.hijacked = true
	server, client := net.Pipe()
	reader := bufio.NewReadWriter(bufio.NewReader(server), bufio.NewWriter(server))
	_ = client.Close()
	return server, reader, nil
}

func (r *hijackableRecorder) Flush() {
	r.flushed = true
	r.ResponseRecorder.Flush()
}

func TestResponseWriterForwardsHijackAndFlush(t *testing.T) {
	base := &hijackableRecorder{ResponseRecorder: httptest.NewRecorder()}
	rw := &responseWriter{ResponseWriter: base}

	if _, _, err := rw.Hijack(); err != nil {
		t.Fatalf("Hijack() error = %v", err)
	}
	if !base.hijacked {
		t.Fatalf("Hijack() did not reach underlying writer")
	}

	rw.Flush()
	if !base.flushed {
		t.Fatalf("Flush() did not reach underlying writer")
	}
}

func TestResponseWriterWriteSetsDefaultStatus(t *testing.T) {
	base := httptest.NewRecorder()
	rw := &responseWriter{ResponseWriter: base}

	if _, err := rw.Write([]byte("ok")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if rw.statusCode != http.StatusOK {
		t.Fatalf("statusCode = %d, want %d", rw.statusCode, http.StatusOK)
	}
}
