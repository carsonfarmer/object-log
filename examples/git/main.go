package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-git/go-git/v6/backend"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	"github.com/go-git/go-git/v6/storage"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
	"go.bytecodealliance.org/pkg/wasihttp"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"io"
	"log"
	"maps"
	"net/http"
	"net/url"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"slices"
	"strings"
)

type loader struct{ s storage.Storer }

func (l loader) Load(*url.URL) (storage.Storer, error) { return l.s, nil }

func sessionRetention(session *wal.Session) (retentionCall, retentionCall) {
	return func(id []byte) (wal.RetentionState, error) {
			return unwrap(func() wt.Result[wal.RetentionState, wal.Failure] { return session.Retain(id) })
		}, func(id []byte) (wal.RetentionState, error) {
			return unwrap(func() wt.Result[wal.RetentionState, wal.Failure] { return session.ReleaseRetention(id) })
		}
}

func advertise(w io.Writer, s *store) error {
	if err := (&packp.SmartReply{Service: transport.ReceivePackService}).Encode(w); err != nil {
		return err
	}
	adv := &packp.AdvRefs{}
	for _, feature := range []string{capability.ReportStatus, capability.DeleteRefs, capability.OFSDelta, capability.Atomic, capability.NoThin} {
		adv.Capabilities.Add(feature)
	}
	adv.Capabilities.Set(capability.ObjectFormat, s.meta.Format.String())
	for _, name := range slices.Sorted(maps.Keys(s.meta.Refs)) {
		id := s.meta.Refs[name]
		adv.References = append(adv.References, plumbing.NewHashReference(plumbing.ReferenceName(name), plumbing.NewHash(id)))
	}
	if len(adv.References) == 0 && s.meta.Format == config.SHA256 {
		// Upstream AdvRefs.Encode uses a SHA-1 zero ID for the empty sentinel.
		if _, err := pktline.Writef(w, "%s capabilities^{}\x00%s\n", strings.Repeat("0", 64), adv.Capabilities.String()); err != nil {
			return err
		}
		return pktline.WriteFlush(w)
	}
	return adv.Encode(w)
}
func init() { wasihttp.HandleFunc(serve) }
func main() {}
func serve(response http.ResponseWriter, r *http.Request) {
	response = componentResponse{response}
	requestID := rand.Text()
	response.Header().Set("X-Request-ID", requestID)
	if r.Body != nil {
		r.Body = &componentBody{ReadCloser: r.Body}
		defer r.Body.Close()
	}
	response.Header().Set("X-Git-Boot-ID", getConfig("GIT_BOOT_ID"))
	response.Header().Set("X-Git-Target-ID", targetID(getConfig))
	if r.URL.Path == "/_validate_backend" {
		if r.Method != http.MethodPost {
			response.Header().Set("Allow", http.MethodPost)
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if status, err := authorizeRequest(r, repositoryRoute{Action: gitAdmin}, getConfig, keyTransport{}); err != nil {
			if status == http.StatusUnauthorized {
				response.Header().Set("WWW-Authenticate", `Basic realm="Git"`)
			}
			http.Error(response, err.Error(), status)
			return
		}
		if _, err := loadRepositories(getConfig); err != nil {
			log.Printf("backend validation failed id=%s: %v", requestID, err)
			http.Error(response, "backend validation failed", http.StatusServiceUnavailable)
			return
		}
		limits, err := loadLimits(getConfig)
		if err != nil {
			log.Printf("backend validation failed id=%s: %v", requestID, err)
			http.Error(response, "backend validation failed", http.StatusServiceUnavailable)
			return
		}
		settings, err := walSettings(getConfig, "backend-validation", limits)
		if err == nil {
			_, err = unwrap(func() wt.Result[wt.Unit, wal.Failure] { return wal.ValidateBackend(settings) })
		}
		if err != nil {
			log.Printf("backend validation failed id=%s: %v", requestID, err)
			http.Error(response, "backend validation failed", http.StatusServiceUnavailable)
			return
		}
		response.WriteHeader(http.StatusNoContent)
		return
	}
	repositories, e := loadRepositories(getConfig)
	if e != nil {
		http.Error(response, e.Error(), http.StatusInternalServerError)
		return
	}
	route, e := resolveRepository(repositories, r)
	if errors.Is(e, errRepositoryMethod) {
		response.Header().Set("Allow", route.Method)
		http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if e != nil {
		http.NotFound(response, r)
		return
	}
	if status, err := authorizeRequest(r, route, getConfig, keyTransport{}); err != nil {
		if status == http.StatusUnauthorized {
			response.Header().Set("WWW-Authenticate", `Basic realm="Git"`)
		}
		http.Error(response, err.Error(), status)
		return
	}
	service, method := route.Service, route.Method
	maintenance := service == "maintenance"
	collect := service == "collect"
	recoverRetentions := service == "recover-retentions-after-drain"
	limits, e := loadLimits(getConfig)
	if e != nil {
		http.Error(response, e.Error(), http.StatusInternalServerError)
		return
	}
	if status := retentionRecoveryStatus(limits.recoverRetentions, recoverRetentions); status != 0 {
		message := "drained retention recovery is disabled"
		if status == http.StatusServiceUnavailable {
			message = "service is draining retained readers"
		}
		http.Error(response, message, status)
		return
	}
	if limits.readOnly && (service == transport.ReceivePackService || maintenance || collect) {
		http.Error(response, "repository is read-only", http.StatusForbidden)
		return
	}
	r, cancel, e := limitedRequest(response, r, limits, service == transport.ReceivePackService && method == http.MethodPost)
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer cancel()
	if err := r.Context().Err(); err != nil {
		http.Error(response, err.Error(), operationStatus(err))
		return
	}
	settings, e := walSettings(getConfig, route.Repository.LogID, limits)
	if e != nil {
		http.Error(response, e.Error(), http.StatusInternalServerError)
		return
	}
	session, e := unwrap(func() wt.Result[*wal.Session, wal.Failure] {
		// First-push discovery requires an empty repository advertisement. Only
		// a configured repository and an authorized writer may create its head.
		if route.Action == gitWrite {
			return wal.Open(settings)
		}
		return wal.OpenExisting(settings)
	})
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer func() { session.Drop() }()
	w := &readResponse{ResponseWriter: response}
	defer w.commit()
	w.Header().Set("Trailer", "X-Wal-Calls, X-Wal-Bytes")
	defer func() {
		u := session.Usage()
		w.Header().Set("X-Wal-Calls", fmt.Sprint(u.Calls))
		w.Header().Set("X-Wal-Bytes", fmt.Sprint(u.Bytes))
		log.Printf("wal %s %s id=%s calls=%d bytes=%d", r.Method, r.URL.Path, requestID, u.Calls, u.Bytes)
	}()
	refresh := func() error {
		if err := r.Context().Err(); err != nil {
			return err
		}
		fresh, err := unwrap(session.Refresh)
		if err == nil {
			session.Drop()
			session = fresh
		}
		return err
	}
	if service == transport.UploadPackService {
		e = retryRead(w, r, refresh, func(attempt *readResponse, request *http.Request) error {
			retain, release := sessionRetention(session)
			return retained(r.Context(), retain, release, func() error {
				s, err := openStore(r.Context(), session, route.Repository, limits)
				if err != nil {
					return err
				}
				defer s.Close()
				if s.stateRoot == nil {
					return errLogMissing
				}
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
		})
		if e != nil {
			log.Printf("git read failed: %v", e)
			if !w.sent {
				http.Error(w, "Git read failed", operationStatus(e))
			}
		}
		return
	}
	if recoverRetentions {
		e = resolveDrainedRecovery(func() (wal.RetentionState, error) {
			return unwrap(session.ClearRetentionsAfterDrain)
		})
		if e != nil {
			http.Error(w, "drained retention recovery failed: "+e.Error(), operationStatus(e))
		} else {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(struct {
				State string `json:"state"`
			}{"complete"})
		}
		return
	}
	if collect || (maintenance && session.HasActiveCollection()) {
		report, err := collectSession(r.Context(), session, limits)
		writeMaintenance(w, report, err)
		return
	}
	open := func() (*store, error) { return openStore(r.Context(), session, route.Repository, limits) }
	s, e := retryOpenStore(open, refresh)
	if e != nil {
		log.Printf("git request setup failed stage=open-store: %v", e)
		http.Error(w, "Git storage unavailable", operationStatus(e))
		return
	}
	defer func() {
		if s != nil {
			s.Close()
		}
	}()
	if s.stateRoot == nil {
		if route.Action != gitWrite {
			http.NotFound(w, r)
			return
		}
		// Persist the format and default branch before the first advertisement.
		// A competing creator must reload the winning root before proceeding.
		if err := s.publish(s.meta.Refs); err != nil && !errors.Is(err, errPublicationConflict) {
			http.Error(w, "repository initialization failed", operationStatus(err))
			return
		}
		s.Close()
		if e = refresh(); e == nil {
			s, e = retryOpenStore(open, refresh)
		}
		if e == nil && s.stateRoot == nil {
			e = errPublicationConflict
		}
		if e != nil {
			http.Error(w, "repository initialization failed", operationStatus(e))
			return
		}
	}
	if service == transport.ReceivePackService && method == http.MethodPost {
		e = retryBeforePush(func() (bool, error) { return s.beforePush() }, func() error {
			s.Close()
			if err := refresh(); err != nil {
				return err
			}
			fresh, err := retryOpenStore(open, refresh)
			if err != nil {
				return err
			}
			s = fresh
			return nil
		})
		if e != nil {
			log.Printf("git request setup failed stage=before-push: %v", e)
			http.Error(w, "Git maintenance unavailable", http.StatusServiceUnavailable)
			return
		}
	}
	if maintenance {
		report, err := s.maintain()
		writeMaintenance(w, report, err)
		return
	} else if service == transport.ReceivePackService && method == http.MethodGet {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		e = advertise(w, s)
	} else if service == transport.ReceivePackService {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		commands := &commandReader{LimitedReader: io.LimitedReader{R: r.Body, N: limits.negotiationBytes}}
		push := &receiveStore{Storer: s, commands: commands}
		e = transport.ReceivePack(r.Context(), push, gitio.NewReadCloser(commands, r.Body), gitio.WriteNopCloser(w), &transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{PreReceive: func(_ context.Context, info *transport.PreReceiveInfo) error {
			if len(info.Commands) == 0 {
				return nil
			}
			refs, e := validate(s, info.Commands)
			if e != nil {
				return e
			}
			e = s.publish(refs)
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
