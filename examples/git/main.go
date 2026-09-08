package main

import (
	"compress/gzip"
	"context"
	"crypto/subtle"
	"encoding/json"
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
	maintenance := service == "maintenance"
	method := http.MethodPost
	if service == "info/refs" {
		service = r.URL.Query().Get("service")
		method = http.MethodGet
	}
	if service != transport.ReceivePackService && service != transport.UploadPackService && !maintenance {
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
	if os.Getenv("GIT_READ_ONLY") == "true" && (service == transport.ReceivePackService || maintenance) {
		http.Error(response, "repository is read-only", http.StatusForbidden)
		return
	}
	limits, e := loadLimits(os.Getenv)
	if e != nil {
		http.Error(response, e.Error(), http.StatusInternalServerError)
		return
	}
	r, cancel, e := limitedRequest(response, r, limits, service == transport.ReceivePackService && method == http.MethodPost)
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer cancel()
	defer r.Body.Close()
	format := config.SHA1
	if parts[0] == "sha256.git" {
		format = config.SHA256
	}
	if err := r.Context().Err(); err != nil {
		http.Error(response, err.Error(), operationStatus(err))
		return
	}
	session, e := unwrap(wal.Open(wal.Config{Endpoint: os.Getenv("WAL_ENDPOINT"), Bucket: os.Getenv("WAL_BUCKET"), Region: os.Getenv("WAL_REGION"), AccessKey: os.Getenv("WAL_ACCESS_KEY"), SecretKey: os.Getenv("WAL_SECRET_KEY"), Prefix: os.Getenv("WAL_PREFIX"), LogId: "repo-" + format.String()}))
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer func() { session.Drop() }()
	w := &httpWriter{ResponseWriter: response}
	w.Header().Set("Trailer", "X-Wal-Calls, X-Wal-Bytes")
	defer func() {
		u := session.Usage()
		w.Header().Set("X-Wal-Calls", fmt.Sprint(u.Calls))
		w.Header().Set("X-Wal-Bytes", fmt.Sprint(u.Bytes))
		log.Printf("wal %s %s calls=%d bytes=%d", r.Method, r.URL.Path, u.Calls, u.Bytes)
	}()
	refresh := func() error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		fresh, err := unwrap(session.Refresh())
		if err == nil {
			session.Drop()
			session = fresh
		}
		return err
	}
	if service == transport.UploadPackService {
		e = retryRead(w, r, refresh, func(attempt *readResponse, request *http.Request) error {
			s, err := openStore(r.Context(), session, format, limits.objectBytes)
			if err != nil {
				return err
			}
			defer s.Close()
			attempt.failure = &s.failure
			if request.Method == http.MethodPost {
				var body io.Reader = request.Body
				if request.Header.Get("Content-Encoding") == "gzip" {
					decoded, err := gzip.NewReader(body)
					if err != nil {
						return err
					}
					defer decoded.Close()
					body = decoded
					request.Header.Del("Content-Encoding")
				}
				tips := make([]plumbing.Hash, 0, len(s.meta.Refs))
				for _, id := range s.meta.Refs {
					tips = append(tips, plumbing.NewHash(id))
				}
				body = http.MaxBytesReader(nil, io.NopCloser(body), limits.negotiationBytes)
				body, err = filterFetch(s, tips, body, strings.Contains(request.Header.Get("Git-Protocol"), "version=2"))
				if err != nil {
					return err
				}
				request.Body = io.NopCloser(body)
			}
			b := backend.New(loader{s})
			b.ErrorLog = log.Default()
			b.ServeHTTP(attempt, request)
			return s.failure
		})
		if e != nil {
			log.Printf("git read failed: %v", e)
			if !w.sent {
				http.Error(w, "Git read failed", operationStatus(e))
			}
		}
		return
	}
	s, e := openStore(r.Context(), session, format, limits.objectBytes)
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer func() { s.Close() }()
	if service == transport.ReceivePackService && method == http.MethodPost {
		reopen, err := s.beforePush()
		if err != nil {
			http.Error(w, err.Error(), http.StatusServiceUnavailable)
			return
		}
		if reopen {
			s.Close()
			if err = refresh(); err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			fresh, err := openStore(r.Context(), session, format, limits.objectBytes)
			if err != nil {
				http.Error(w, err.Error(), http.StatusServiceUnavailable)
				return
			}
			s = fresh
		}
	}
	if maintenance {
		report, err := s.maintain()
		if err != nil {
			http.Error(w, "maintenance failed: "+err.Error(), operationStatus(err))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		states := map[wal.MaintenanceState]string{wal.MaintenanceStateComplete: "complete", wal.MaintenanceStateMore: "more", wal.MaintenanceStateConflict: "conflict", wal.MaintenanceStatePending: "pending", wal.MaintenanceStateRetained: "retained"}
		e = json.NewEncoder(w).Encode(struct {
			State   string `json:"state"`
			Objects uint64 `json:"candidate_objects"`
			Bytes   uint64 `json:"candidate_bytes"`
		}{states[report.State], report.Objects, report.Bytes})
	} else if service == transport.ReceivePackService && method == http.MethodGet {
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
	}
	if e != nil {
		log.Printf("git request failed: %v", e)
		if !w.sent {
			http.Error(w, "Git operation failed", operationStatus(e))
		}
	} else if !w.sent {
		w.WriteHeader(http.StatusOK)
	}
}
