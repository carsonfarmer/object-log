package transport_test

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Opt-in local test: scripts/test-git-auth-transport.sh. Uses stock Spin and
// production auth_transport.go, without loading the WAL service or using AWS.
func TestSpinAuthTransport(t *testing.T) {
	tmp := t.TempDir()
	write := func(name string, data []byte) {
		t.Helper()
		if err := os.WriteFile(filepath.Join(tmp, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	copyFile := func(source, name string) {
		t.Helper()
		data, err := os.ReadFile(source)
		if err != nil {
			t.Fatal(err)
		}
		write(name, data)
	}
	_, source, _, _ := runtime.Caller(0)
	copyFile(filepath.Join(filepath.Dir(source), "component.go.txt"), "main.go")
	for _, name := range []string{"auth_transport.go", "go.mod", "go.sum"} {
		copyFile(filepath.Join(os.Getenv("GIT_TRANSPORT_SOURCE"), name), name)
	}
	build := exec.Command(os.Getenv("COMPONENTIZE_GO"), "-w", "bytecodealliance:pkg/wasip2", "build")
	build.Dir = tmp
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, output)
	}

	canceled := make(chan string, 2)
	upstream := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "" {
			t.Error("transport forwarded credentials")
		}
		switch r.URL.Path {
		case "/success":
			io.WriteString(w, "keys")
		case "/stall":
			select {
			case <-r.Context().Done():
				canceled <- r.URL.Path
			case <-time.After(8 * time.Second):
			}
		case "/drip":
			for range 40 {
				io.WriteString(w, "x")
				w.(http.Flusher).Flush()
				select {
				case <-r.Context().Done():
					canceled <- r.URL.Path
					return
				case <-time.After(250 * time.Millisecond):
				}
			}
		case "/oversize":
			io.WriteString(w, strings.Repeat("x", (64<<10)+1))
		case "/truncated":
			w.Header().Set("Content-Length", "100")
			io.WriteString(w, "x") // Closing before Content-Length produces a body error.
			w.(http.Flusher).Flush()
		}
	}))
	certificate, ca := fixtureCertificate(t)
	upstream.TLS = &tls.Config{Certificates: []tls.Certificate{certificate}}
	upstream.StartTLS()
	defer upstream.Close()
	write("ca.pem", pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: ca}))
	write("runtime.toml", []byte(`[[client_tls]]
component_ids = ["probe"]
hosts = ["127.0.0.1"]
ca_roots_file = "ca.pem"
ca_use_platform_roots = false
`))
	write("spin.toml", []byte(fmt.Sprintf(`spin_manifest_version = 2
[application]
name = "auth-transport-test"
version = "0.0.0"
[[trigger.http]]
route = "/..."
component = "probe"
[component.probe]
source = "main.wasm"
allowed_outbound_hosts = [%q]
environment = { FIXTURE_URL = %q }
`, upstream.URL, upstream.URL)))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	listener.Close()
	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	spin := exec.CommandContext(ctx, "spin", "up", "--from", "spin.toml", "--runtime-config-file", "runtime.toml", "--listen", address)
	spin.Dir = tmp
	log, err := os.Create(filepath.Join(tmp, "spin.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	spin.Stdout, spin.Stderr = log, log
	if err := spin.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stop()
		spin.Wait()
		if t.Failed() {
			data, _ := os.ReadFile(log.Name())
			t.Logf("Spin log:\n%s", data)
		}
	}()
	client := &http.Client{Timeout: 12 * time.Second}
	base := "http://" + address
	ready := false
	for range 200 {
		response, err := client.Get(base + "/ready")
		if err == nil {
			response.Body.Close()
			ready = response.StatusCode == http.StatusOK
		}
		if ready {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ready {
		t.Fatal("Spin never became ready")
	}
	for _, test := range []struct {
		path, want string
		deadline   bool
	}{
		{"/success", "keys", false},
		{"/stall", "context deadline exceeded", true},
		{"/drip", "context deadline exceeded", true},
		{"/oversize", "JWKS exceeds byte limit", false},
		{"/truncated", "authentication keys unavailable", false},
	} {
		t.Run(test.path, func(t *testing.T) {
			response, err := client.Get(base + test.path)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var got struct {
				Result   string
				Elapsed  int64
				Followup string
			}
			if err := json.NewDecoder(response.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if got.Result != test.want || got.Followup != "keys" {
				t.Fatalf("got %+v, want %q then keys", got, test.want)
			}
			if test.deadline && (got.Elapsed < 4500 || got.Elapsed > 6500) {
				t.Fatalf("deadline took %dms", got.Elapsed)
			}
			if test.deadline {
				select {
				case path := <-canceled:
					if path != test.path {
						t.Fatalf("canceled %s, want %s", path, test.path)
					}
				case <-time.After(time.Second):
					t.Fatal("upstream request remained active after transport returned")
				}
			}
			t.Logf("%s in %dms; same-instance followup: %s", got.Result, got.Elapsed, got.Followup)
		})
	}
}

// httptest's default certificate is itself a CA, which rustls correctly refuses
// as an end entity. Use a dedicated leaf signed by this test's private CA.
func fixtureCertificate(t *testing.T) (tls.Certificate, []byte) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "transport fixture CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	root, err := x509.CreateCertificate(rand.Reader, ca, ca, public, private)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "localhost"},
		NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
		KeyUsage:    x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, leaf, ca, public, private)
	if err != nil {
		t.Fatal(err)
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: private}, root
}
