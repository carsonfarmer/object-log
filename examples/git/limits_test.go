package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestRequestLimits(t *testing.T) {
	defaults, err := loadLimits(func(string) string { return "" })
	if err != nil || defaults.pushBytes != 2<<30 || defaults.objectBytes != 1<<30 || defaults.metadataBytes != 16<<20 || defaults.packObjects != 1_000_000 || defaults.catalogBytes != 64<<20 {
		t.Fatalf("defaults: %+v %v", defaults, err)
	}
	for _, key := range []string{"GIT_MAX_PUSH_BYTES", "GIT_MAX_NEGOTIATION_BYTES", "GIT_MAX_OBJECT_BYTES", "GIT_MAX_METADATA_BYTES", "GIT_MAX_CATALOG_BYTES", "GIT_MAX_PACK_OBJECTS", "GIT_REQUEST_TIMEOUT"} {
		for _, value := range []string{"0", "-1", "invalid", "9223372036854775808"} {
			_, err := loadLimits(func(name string) string {
				if name == key {
					return value
				}
				return ""
			})
			if err == nil {
				t.Errorf("accepted %s=%s", key, value)
			}
		}
	}
	for _, chunked := range []bool{false, true} {
		for _, size := range []int{4, 5} {
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(strings.Repeat("x", size)))
			if chunked {
				req.ContentLength = -1
			}
			bounded, cancel, err := limitedRequest(httptest.NewRecorder(), req, requestLimits{pushBytes: 4, timeout: time.Minute}, true)
			if err == nil {
				_, err = io.ReadAll(bounded.Body)
				bounded.Body.Close()
				cancel()
			}
			if size == 4 && err != nil {
				t.Fatal(err)
			}
			if size == 5 && operationStatus(err) != http.StatusRequestEntityTooLarge {
				t.Fatalf("size=%d chunked=%v: %v", size, chunked, err)
			}
		}
	}
}

func TestRequestDeadline(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("data")).WithContext(ctx)
	bounded, stop, err := limitedRequest(httptest.NewRecorder(), request, requestLimits{negotiationBytes: 8, timeout: time.Minute}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	defer bounded.Body.Close()
	cancel()
	if _, err := io.ReadAll(bounded.Body); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled body: %v", err)
	}
	expired, finish := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer finish()
	body := &requestBody{ReadCloser: io.NopCloser(strings.NewReader("data")), ctx: expired}
	if _, err := io.ReadAll(body); !errors.Is(err, context.DeadlineExceeded) || operationStatus(err) != http.StatusRequestTimeout {
		t.Fatalf("expired body: %v", err)
	}
}

func TestReadOnlyConfiguration(t *testing.T) {
	for _, value := range []string{"true", "TRUE", "1", "false", "FALSE", "0", "tru"} {
		limits, err := loadLimits(func(key string) string {
			if key == "GIT_READ_ONLY" {
				return value
			}
			return ""
		})
		if value == "tru" {
			if err == nil {
				t.Fatal("invalid read-only setting accepted")
			}
			continue
		}
		want := value == "true" || value == "TRUE" || value == "1"
		if err != nil || limits.readOnly != want {
			t.Fatalf("%s: %v %v", value, limits.readOnly, err)
		}
	}
}

func TestCatalogReadBudget(t *testing.T) {
	limits, err := loadLimits(func(key string) string {
		if key == "GIT_MAX_CATALOG_BYTES" {
			return "10"
		}
		return ""
	})
	if err != nil {
		t.Fatal(err)
	}
	// Reopening a store copies limits, but cannot restore the consumed budget.
	first, retry := limits, limits
	if err := first.chargeCatalog(3); err != nil {
		t.Fatal(err)
	}
	if err := retry.chargeCatalog(7); err != nil {
		t.Fatal(err)
	}
	if *limits.catalogRead != 10 {
		t.Fatalf("charged %d", *limits.catalogRead)
	}
	if err := first.chargeCatalog(1); !errors.Is(err, errObjectLimit) || *retry.catalogRead != 10 || operationStatus(err) != http.StatusRequestEntityTooLarge {
		t.Fatalf("overflow: used=%d err=%v", *retry.catalogRead, err)
	}
	fresh, err := loadLimits(func(string) string { return "" })
	if err != nil || *fresh.catalogRead != 0 {
		t.Fatal("new request inherited budget")
	}
	limits.catalogBytes = 1<<63 - 1
	*limits.catalogRead = limits.catalogBytes - 1
	if err := limits.chargeCatalog(2); !errors.Is(err, errObjectLimit) || *limits.catalogRead != limits.catalogBytes-1 {
		t.Fatalf("integer overflow: used=%d err=%v", *limits.catalogRead, err)
	}
}
