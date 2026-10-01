package main

import (
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/go-git/go-git/v6/plumbing"
	"github.com/go-git/go-git/v6/plumbing/format/config"
	"github.com/go-git/go-git/v6/plumbing/format/pktline"
	"github.com/go-git/go-git/v6/plumbing/protocol/capability"
	"github.com/go-git/go-git/v6/plumbing/protocol/packp"
	"github.com/go-git/go-git/v6/plumbing/transport"
	gitio "github.com/go-git/go-git/v6/utils/ioutil"
	"go.bytecodealliance.org/pkg/wasihttp"
	wt "go.bytecodealliance.org/pkg/wit/types"
	"io"
	"log"
	"maps"
	"net/http"
	wal "object-log-git-proof/bindings/object_log_storage_wal"
	"slices"
	"strings"
)

func sessionRetention(session *wal.Session) (retentionCall, retentionCall) {
	return func(id []byte) (wal.RetentionState, error) {
			return unwrap(func() wt.Result[wal.RetentionState, wal.Failure] { return session.Retain(id) })
		}, func(id []byte) (wal.RetentionState, error) {
			return unwrap(func() wt.Result[wal.RetentionState, wal.Failure] { return session.ReleaseRetention(id) })
		}
}

func advertise(w io.Writer, s *store, unknownFormat bool) error {
	if err := (&packp.SmartReply{Service: transport.ReceivePackService}).Encode(w); err != nil {
		return err
	}
	adv := &packp.AdvRefs{}
	capability.DecodeList([]byte("report-status delete-refs ofs-delta atomic no-thin side-band-64k quiet"), &adv.Capabilities)
	adv.Capabilities.Set(capability.ObjectFormat, s.meta.Format.String())
	if unknownFormat {
		adv.Capabilities.Set(capability.ObjectFormat, "sha1", "sha256")
	}
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
	repositories, configErr := loadRepositories(getConfig)
	route, e := resolveRepository(repositories, r)
	if configErr != nil && route.Service != "validate-backend" {
		http.Error(response, configErr.Error(), http.StatusInternalServerError)
		return
	}
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
	e = errors.Join(configErr, e)
	if service == "validate-backend" {
		if e == nil {
			settings, err := walSettings(getConfig, "backend-validation", limits)
			e = err
			if e == nil {
				_, e = unwrap(func() wt.Result[wt.Unit, wal.Failure] { return wal.ValidateBackend(settings) })
			}
		}
		if e != nil {
			log.Printf("backend validation failed id=%s: %v", requestID, e)
			http.Error(response, "backend validation failed", http.StatusServiceUnavailable)
		} else {
			response.WriteHeader(http.StatusNoContent)
		}
		return
	}
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
	if limits.readOnly && (route.Action == gitWrite || maintenance || collect) {
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
	if service == "authorize-read" {
		response.Header().Set("Cache-Control", "no-store")
		log.Printf("wal %s %s id=%s calls=0 bytes=0", r.Method, r.URL.Path, requestID)
		response.WriteHeader(http.StatusNoContent)
		return
	}
	var pushCapabilities *capability.List
	var pushFormat config.ObjectFormat
	if service == transport.ReceivePackService && method == http.MethodPost {
		format, capabilities, body, err := receiveFormat(r.Body, limits.negotiationBytes)
		if err != nil {
			status := operationStatus(err)
			if status == http.StatusInternalServerError {
				status = http.StatusBadRequest
			}
			http.Error(response, "invalid push format", status)
			return
		}
		if format == "" {
			response.Header().Set("Content-Type", "application/x-git-receive-pack-result")
			log.Printf("wal %s %s id=%s calls=0 bytes=0", r.Method, r.URL.Path, requestID)
			response.WriteHeader(http.StatusOK)
			return
		}
		pushFormat = format
		pushCapabilities = capabilities
		r.Body = gitio.NewReadCloser(body, r.Body)
	}

	settings, e := walSettings(getConfig, route.LogID, limits)
	if e != nil {
		http.Error(response, e.Error(), http.StatusInternalServerError)
		return
	}
	openSession := wal.OpenExisting
	if service == transport.ReceivePackService && method == http.MethodPost {
		openSession = wal.Open
	}
	session, e := unwrap(func() wt.Result[*wal.Session, wal.Failure] { return openSession(settings) })
	if errors.Is(e, errLogMissing) && service == transport.ReceivePackService && method == http.MethodGet {
		response.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		if err := advertise(response, &store{meta: rootMeta{Format: config.SHA1}}, true); err != nil {
			log.Printf("git discovery failed: %v", err)
		}
		return
	}
	if e != nil {
		http.Error(response, e.Error(), operationStatus(e))
		return
	}
	defer func() { session.Drop() }()
	w := &readResponse{ResponseWriter: response}
	defer w.WriteHeader(http.StatusOK)
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
		capabilitiesOnly := method == http.MethodGet && r.Header.Get("Git-Protocol") == "version=2"
		run := func() error {
			open := func() (*store, error) { return openStore(r.Context(), session, pushFormat, limits) }
			var s *store
			var err error
			if capabilitiesOnly {
				s, err = retryOpenStore(open, refresh)
			} else {
				s, err = open()
			}
			if err != nil {
				return err
			}
			defer s.Close()
			if s.stateRoot == nil {
				return errLogMissing
			}
			var body io.Reader = r.Body
			if method == http.MethodPost {
				if r.Header.Get("Content-Encoding") == "gzip" {
					decoded, err := gzip.NewReader(body)
					if err != nil {
						return err
					}
					defer decoded.Close()
					body = decoded
				}
				tips := make([]plumbing.Hash, 0, len(s.meta.Refs))
				for _, id := range s.meta.Refs {
					tips = append(tips, plumbing.NewHash(id))
				}
				body = http.MaxBytesReader(nil, io.NopCloser(body), limits.negotiationBytes)
				body, err = filterFetch(s, tips, body, strings.Contains(r.Header.Get("Git-Protocol"), "version=2"))
				if err != nil {
					return err
				}
			}
			if method == http.MethodGet {
				w.Header().Set("Expires", "Fri, 01 Jan 1980 00:00:00 GMT")
				w.Header().Set("Pragma", "no-cache")
				w.Header().Set("Cache-Control", "no-cache, max-age=0, must-revalidate")
				w.Header().Set("Content-Type", "application/x-git-upload-pack-advertisement")
			} else {
				if strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type"))) != "application/x-git-upload-pack-request" {
					http.Error(w, "403 Forbidden", http.StatusForbidden)
					return nil
				}
				w.Header().Set("Content-Type", "application/x-git-upload-pack-result")
				w.Header().Set("X-Content-Type-Options", "nosniff")
			}
			err = transport.UploadPack(r.Context(), s, io.NopCloser(body), gitio.WriteNopCloser(&failedWriter{w, &s.failure}), &transport.UploadPackRequest{
				GitProtocol: r.Header.Get("Git-Protocol"), AdvertiseRefs: method == http.MethodGet, StatelessRPC: true,
			})
			if err != nil && s.failure == nil && !w.sent {
				http.Error(w, "500 Internal Server Error", http.StatusInternalServerError)
			}
			return errors.Join(s.failure, err)
		}
		// Retention advances the session before recovery and prevents view expiry.
		// V2 discovery reads only metadata, so it retries recovery without retention.
		if capabilitiesOnly {
			e = run()
		} else {
			retain, release := sessionRetention(session)
			e = retained(r.Context(), retain, release, run)
		}
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
	open := func() (*store, error) { return openStore(r.Context(), session, pushFormat, limits) }
	s, e := retryOpenStore(open, refresh)
	if e != nil {
		log.Printf("git request setup failed stage=open-store: %v", e)
		status := operationStatus(e)
		if errors.Is(e, config.ErrInvalidObjectFormat) {
			status = http.StatusBadRequest
		}
		http.Error(w, "Git storage unavailable", status)
		return
	}
	defer func() { s.Close() }()
	if maintenance {
		report, err := s.maintain()
		writeMaintenance(w, report, err)
		return
	}
	if method == http.MethodPost {
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
	if method == http.MethodGet {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-advertisement")
		e = advertise(w, s, s.stateRoot == nil)
	} else {
		w.Header().Set("Content-Type", "application/x-git-receive-pack-result")
		s.progress = newReceiveProgress(w, pushCapabilities, cancel)
		commands := &commandReader{LimitedReader: io.LimitedReader{R: r.Body, N: limits.negotiationBytes}}
		push := &receiveStore{Storer: s, commands: commands}
		e = transport.ReceivePack(r.Context(), push, gitio.NewReadCloser(commands, r.Body), gitio.WriteNopCloser(w), &transport.ReceivePackRequest{StatelessRPC: true, Hooks: transport.ReceivePackHooks{PreReceive: func(_ context.Context, info *transport.PreReceiveInfo) error {
			if s.progress != nil {
				s.progress.writer = info.Progress
			}
			if err := s.progress.message("Validating update...\n"); err != nil {
				return err
			}
			refs, e := validate(s, info.Commands)
			if e != nil {
				return e
			}
			if s.stateRoot == nil {
				for _, command := range info.Commands {
					if command.Action() != packp.Delete && command.Name.IsBranch() {
						s.meta.Head = command.Name.String()
						break
					}
				}
			}
			if err := s.progress.message("Publishing update...\n"); err != nil {
				return err
			}
			return s.publish(refs)
		}}})
	}
	if e != nil {
		log.Printf("git request failed: %v", e)
		if !w.sent {
			http.Error(w, "Git operation failed", operationStatus(e))
		}
	}
}
