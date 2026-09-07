package main

import (
	"context"
	"crypto/subtle"
	"fmt"
	"github.com/go-git/go-git/v6/backend"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	"go.bytecodealliance.org/pkg/wasihttp"
	"io"
	"log"
	"net/http"
	"net/url"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"os"
	"strings"
)

type loader struct{ s storage.Storer }

func (l loader) Load(*url.URL) (storage.Storer, error) { return l.s, nil }

type frozen struct{ *store }

func (*frozen) SetReference(*plumbing.Reference) error       { return nil }
func (*frozen) RemoveReference(plumbing.ReferenceName) error { return nil }

type writeCloser struct{ io.Writer }

func (writeCloser) Close() error { return nil }

type httpWriter struct {
	http.ResponseWriter
	sent bool
}

func (w *httpWriter) strip() { w.Header().Del("Connection"); w.Header().Del("Transfer-Encoding") }
func (w *httpWriter) Write(p []byte) (int, error) {
	w.strip()
	w.sent = true
	return w.ResponseWriter.Write(p)
}
func (w *httpWriter) WriteHeader(status int) {
	w.strip()
	w.sent = true
	w.ResponseWriter.WriteHeader(status)
}
func advertise(w io.Writer, s *store) error {
	if _, e := pktline.WriteString(w, "# service=git-receive-pack\n"); e != nil {
		return e
	}
	if e := pktline.WriteFlush(w); e != nil {
		return e
	}
	caps := "report-status delete-refs ofs-delta atomic object-format=" + s.meta.Format.String()
	first := true
	for name, id := range s.meta.Refs {
		suffix := ""
		if first {
			suffix = "\x00" + caps
			first = false
		}
		if _, e := pktline.Writef(w, "%s %s%s\n", id, name, suffix); e != nil {
			return e
		}
	}
	if first {
		size := 40
		if s.meta.Format == config.SHA256 {
			size = 64
		}
		if _, e := pktline.Writef(w, "%s capabilities^{}\x00%s\n", strings.Repeat("0", size), caps); e != nil {
			return e
		}
	}
	return pktline.WriteFlush(w)
}
func init() { wasihttp.HandleFunc(serve) }
func main() {}
func serve(response http.ResponseWriter, r *http.Request) {
	parts := strings.SplitN(strings.TrimPrefix(r.URL.Path, "/"), "/", 2)
	if len(parts) != 2 || (parts[0] != "sha1.git" && parts[0] != "sha256.git") {
		http.NotFound(response, r)
		return
	}
	service := parts[1]
	method := http.MethodPost
	if service == "info/refs" {
		service = r.URL.Query().Get("service")
		method = http.MethodGet
	}
	if service != transport.ReceivePackService && service != transport.UploadPackService {
		http.NotFound(response, r)
		return
	}
	if r.Method != method {
		response.Header().Set("Allow", method)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if password := os.Getenv("GIT_PASSWORD"); password != "" {
		_, supplied, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(password), []byte(supplied)) != 1 {
			response.Header().Set("WWW-Authenticate", `Basic realm="Git"`)
			http.Error(response, "authentication required", http.StatusUnauthorized)
			return
		}
	}
	if os.Getenv("GIT_READ_ONLY") == "true" && service == transport.ReceivePackService {
		http.Error(response, "repository is read-only", http.StatusForbidden)
		return
	}
	format := config.SHA1
	if parts[0] == "sha256.git" {
		format = config.SHA256
	}
	session, e := unwrap(wal.Open(wal.Config{Endpoint: os.Getenv("WAL_ENDPOINT"), Bucket: os.Getenv("WAL_BUCKET"), Region: os.Getenv("WAL_REGION"), AccessKey: os.Getenv("WAL_ACCESS_KEY"), SecretKey: os.Getenv("WAL_SECRET_KEY"), Prefix: os.Getenv("WAL_PREFIX"), LogId: "repo-" + format.String()}))
	if e != nil {
		http.Error(response, e.Error(), 500)
		return
	}
	defer session.Drop()
	s, e := openStore(session, format)
	if e != nil {
		http.Error(response, e.Error(), 500)
		return
	}
	defer s.Close()
	w := &httpWriter{ResponseWriter: response}
	w.Header().Set("Trailer", "X-Wal-Calls, X-Wal-Bytes")
	defer func() {
		u := session.Usage()
		w.Header().Set("X-Wal-Calls", fmt.Sprint(u.Calls))
		w.Header().Set("X-Wal-Bytes", fmt.Sprint(u.Bytes))
		log.Printf("wal %s %s calls=%d bytes=%d", r.Method, r.URL.Path, u.Calls, u.Bytes)
	}()
	if service == transport.ReceivePackService && method == http.MethodGet {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		e = advertise(w, s)
	} else if service == transport.ReceivePackService {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		e = receive(r.Context(), &frozen{s}, r.Body, writeCloser{w}, &transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{PreReceive: func(_ context.Context, info *transport.PreReceiveInfo) error {
			refs, e := validate(s, info.Commands)
			if e != nil {
				return e
			}
			e = s.publish(refs)
			if pending, ok := e.(*pendingError); ok {
				w.Header().Set("X-Wal-Recovery-Token", fmt.Sprintf("%x", pending.token))
			}
			return e
		}}})
	} else {
		b := backend.New(loader{s})
		b.ErrorLog = log.Default()
		b.ServeHTTP(w, r)
	}
	if e != nil {
		log.Printf("git request failed: %v", e)
		if !w.sent {
			http.Error(w, "Git operation failed", http.StatusInternalServerError)
		}
	} else if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
}
