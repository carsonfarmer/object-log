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

	"github.com/go-git/go-git/v6/plumbing"
)

func TestObjectAndMetadataLimits(t *testing.T) {
	limits := requestLimits{objectBytes: 32, metadataBytes: 16}
	for _, kind := range []plumbing.ObjectType{plumbing.BlobObject, plumbing.CommitObject, plumbing.TreeObject, plumbing.TagObject} {
		for _, size := range []int64{16, 17, 32, 33} {
			err := limits.checkObject(kind, size)
			if err != nil && !errors.Is(err, errObjectLimit) {
				t.Fatalf("%s size %d: unexpected error %v", kind, size, err)
			}
			wantLimit := size > 32 || (kind != plumbing.BlobObject && size > 16)
			if errors.Is(err, errObjectLimit) != wantLimit {
				t.Fatalf("%s size %d: got %v, want limit=%v", kind, size, err, wantLimit)
			}
		}
	}
}

func TestRequestLimits(t *testing.T) {
	defaults, err := loadLimits(func(string) string { return "" })
	if err != nil || defaults.pushBytes != 2<<30 || defaults.objectBytes != 64<<20 || defaults.metadataBytes != 16<<20 || defaults.packObjects != 1_000_000 || defaults.catalogBytes != 64<<20 || defaults.collectionObjects != 100_000 {
		t.Fatalf("defaults: %+v %v", defaults, err)
	}
	for _, key := range []string{"GIT_MAX_PUSH_BYTES", "GIT_MAX_NEGOTIATION_BYTES", "GIT_MAX_OBJECT_BYTES", "GIT_MAX_METADATA_BYTES", "GIT_MAX_CATALOG_BYTES", "GIT_MAX_PACK_OBJECTS", "WAL_MAX_COLLECTION_OBJECTS", "WAL_COLLECTION_CANDIDATES", "GIT_REQUEST_TIMEOUT"} {
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
	if operationStatus(errObjectLimit) != http.StatusRequestEntityTooLarge {
		t.Fatal("receive command limit has wrong HTTP status")
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
	for _, key := range []string{"GIT_READ_ONLY", "WAL_RECOVER_RETENTIONS_AFTER_DRAIN"} {
		for _, value := range []string{"true", "TRUE", "1", "false", "FALSE", "0", "tru"} {
			limits, err := loadLimits(func(name string) string {
				if name == key {
					return value
				}
				return ""
			})
			if value == "tru" {
				if err == nil {
					t.Fatalf("invalid %s setting accepted", key)
				}
				continue
			}
			want := value == "true" || value == "TRUE" || value == "1"
			got := limits.readOnly
			if key == "WAL_RECOVER_RETENTIONS_AFTER_DRAIN" {
				got = limits.recoverRetentions
			}
			if err != nil || got != want {
				t.Fatalf("%s=%s: %v %v", key, value, got, err)
			}
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
