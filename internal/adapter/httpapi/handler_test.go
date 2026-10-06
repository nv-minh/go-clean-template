package httpapi_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/yourorg/go-clean-template/internal/adapter/httpapi"
	"github.com/yourorg/go-clean-template/internal/domain"
	"github.com/yourorg/go-clean-template/internal/platform/metrics"
	"github.com/yourorg/go-clean-template/internal/usecase"
)

var (
	custID = uuid.MustParse("0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b")
	t0     = time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
)

type fakeSvc struct {
	mu        sync.Mutex
	createN   int
	createErr error
	getErr    error
	getPanic  bool
	order     *domain.Order
	listRes   *usecase.ListResult
}

func (f *fakeSvc) Create(_ context.Context, in usecase.CreateOrderInput) (*domain.Order, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.createN++
	if f.createErr != nil {
		return nil, f.createErr
	}
	o := *f.order
	o.CustomerID = in.CustomerID
	return &o, nil
}

func (f *fakeSvc) Get(context.Context, uuid.UUID) (*domain.Order, error) {
	if f.getPanic {
		panic("boom")
	}
	return f.order, f.getErr
}

func (f *fakeSvc) List(context.Context, uuid.UUID, *domain.Cursor, int) (*usecase.ListResult, error) {
	return f.listRes, nil
}

func (f *fakeSvc) Cancel(context.Context, uuid.UUID) (*domain.Order, error) { return f.order, nil }

type memIdem struct {
	mu    sync.Mutex
	state map[string]*memRec
}

type memRec struct {
	fp   string
	done bool
	resp httpapi.SavedResponse
}

func (m *memIdem) Begin(_ context.Context, key, fp string) (httpapi.IdemResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == nil {
		m.state = map[string]*memRec{}
	}
	r, ok := m.state[key]
	switch {
	case !ok:
		m.state[key] = &memRec{fp: fp}
		return httpapi.IdemResult{State: httpapi.IdemStarted}, nil
	case r.fp != fp:
		return httpapi.IdemResult{State: httpapi.IdemMismatch}, nil
	case !r.done:
		return httpapi.IdemResult{State: httpapi.IdemInFlight}, nil
	default:
		return httpapi.IdemResult{State: httpapi.IdemReplay, Saved: &r.resp}, nil
	}
}

func (m *memIdem) Complete(_ context.Context, key, _ string, resp httpapi.SavedResponse) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key].done, m.state[key].resp = true, resp
	return nil
}

func (m *memIdem) Abort(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.state, key)
	return nil
}

type fakeLimiter struct {
	allowed bool
	err     error
}

func (l fakeLimiter) Allow(context.Context, string) (bool, time.Duration, error) {
	return l.allowed, 1500 * time.Millisecond, l.err
}

func newRouter(svc *fakeSvc, mutate func(*httpapi.Deps)) http.Handler {
	d := httpapi.Deps{
		Orders:  svc,
		Log:     slog.New(slog.DiscardHandler),
		Metrics: metrics.New("test"),
		Options: httpapi.Options{RequestTimeout: 2 * time.Second, MaxBodyBytes: 1 << 10},
	}
	if mutate != nil {
		mutate(&d)
	}
	return httpapi.NewRouter(d)
}

func newOrder() *domain.Order {
	return &domain.Order{
		ID: uuid.MustParse("01a10d3e-b6c3-7975-ba38-9406719bb906"), CustomerID: custID, Status: domain.StatusPending,
		Currency: "USD", TotalAmount: 200, Items: []domain.Item{{SKU: "A", Quantity: 2, UnitPrice: 100}}, Version: 3,
		CreatedAt: t0, UpdatedAt: t0,
	}
}

const validBody = `{"customer_id":"0197a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5b","currency":"USD","items":[{"sku":"A","quantity":2,"unit_price":100}]}`

func do(h http.Handler, method, target, body string, hdr map[string]string) *httptest.ResponseRecorder {
	req := httptest.NewRequestWithContext(context.Background(), method, target, strings.NewReader(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestCreate(t *testing.T) {
	t.Parallel()
	h := newRouter(&fakeSvc{order: newOrder()}, nil)
	rec := do(h, http.MethodPost, "/v1/orders", validBody, nil)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status %d: %s", rec.Code, rec.Body)
	}
	if loc := rec.Header().Get("Location"); loc != "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906" {
		t.Fatalf("location %q", loc)
	}
	if rec.Header().Get("X-Request-Id") == "" || rec.Header().Get("X-Content-Type-Options") != "nosniff" {
		t.Fatal("missing request id or security headers")
	}
}

func TestCreateBadRequests(t *testing.T) {
	t.Parallel()
	h := newRouter(&fakeSvc{order: newOrder()}, nil)
	tests := []struct {
		name string
		body string
		want int
	}{
		{"malformed json", `{`, http.StatusBadRequest},
		{"unknown field", `{"nope":1}`, http.StatusBadRequest},
		{"trailing data", validBody + `{}`, http.StatusBadRequest},
		{"bad uuid", `{"customer_id":"x"}`, http.StatusBadRequest},
		{"too large", `{"currency":"` + strings.Repeat("A", 2<<10) + `"}`, http.StatusRequestEntityTooLarge},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if rec := do(h, http.MethodPost, "/v1/orders", tc.body, nil); rec.Code != tc.want {
				t.Fatalf("want %d, got %d: %s", tc.want, rec.Code, rec.Body)
			}
		})
	}
}

func TestErrorMapping(t *testing.T) {
	t.Parallel()
	tests := []struct {
		err  error
		want int
	}{
		{domain.ErrInvalid, http.StatusUnprocessableEntity},
		{domain.ErrNotFound, http.StatusNotFound},
		{domain.ErrConflict, http.StatusConflict},
		{domain.ErrInvalidState, http.StatusConflict},
		{context.DeadlineExceeded, http.StatusGatewayTimeout},
		{errors.New("pq: password authentication failed for user admin"), http.StatusInternalServerError},
	}
	for _, tc := range tests {
		t.Run(tc.err.Error(), func(t *testing.T) {
			t.Parallel()
			rec := do(newRouter(&fakeSvc{order: newOrder(), createErr: tc.err}, nil), http.MethodPost, "/v1/orders", validBody, nil)
			if rec.Code != tc.want {
				t.Fatalf("want %d, got %d", tc.want, rec.Code)
			}
			if tc.want == http.StatusInternalServerError && strings.Contains(rec.Body.String(), "password") {
				t.Fatal("internal error details leaked to the client")
			}
		})
	}
}

func TestGetETag(t *testing.T) {
	t.Parallel()
	h := newRouter(&fakeSvc{order: newOrder()}, nil)
	url := "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906"
	rec := do(h, http.MethodGet, url, "", nil)
	if rec.Code != http.StatusOK || rec.Header().Get("ETag") != `W/"3"` {
		t.Fatalf("status %d etag %q", rec.Code, rec.Header().Get("ETag"))
	}
	if rec := do(h, http.MethodGet, url, "", map[string]string{"If-None-Match": `W/"3"`}); rec.Code != http.StatusNotModified || rec.Body.Len() != 0 {
		t.Fatalf("want empty 304, got %d (%d bytes)", rec.Code, rec.Body.Len())
	}
	if rec := do(h, http.MethodGet, "/v1/orders/not-a-uuid", "", nil); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestListValidationAndCursor(t *testing.T) {
	t.Parallel()
	o := newOrder()
	h := newRouter(&fakeSvc{order: o, listRes: &usecase.ListResult{Orders: []*domain.Order{o}, Next: &domain.Cursor{CreatedAt: t0, ID: o.ID}}}, nil)

	if rec := do(h, http.MethodGet, "/v1/orders", "", nil); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("missing customer_id: %d", rec.Code)
	}
	if rec := do(h, http.MethodGet, "/v1/orders?customer_id="+custID.String()+"&cursor=!!!", "", nil); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("bad cursor: %d", rec.Code)
	}
	rec := do(h, http.MethodGet, "/v1/orders?customer_id="+custID.String()+"&limit=1", "", nil)
	var out struct {
		Data       []json.RawMessage `json:"data"`
		NextCursor string            `json:"next_cursor"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil || len(out.Data) != 1 || out.NextCursor == "" {
		t.Fatalf("unexpected list response %d: %s", rec.Code, rec.Body)
	}
	// The returned cursor must be accepted back.
	if rec := do(h, http.MethodGet, "/v1/orders?customer_id="+custID.String()+"&cursor="+out.NextCursor, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("cursor round trip: %d %s", rec.Code, rec.Body)
	}
}

func TestIdempotency(t *testing.T) {
	t.Parallel()
	svc := &fakeSvc{order: newOrder()}
	h := newRouter(svc, func(d *httpapi.Deps) { d.Idempotency = &memIdem{} })
	hdr := map[string]string{"Idempotency-Key": "k1"}

	first := do(h, http.MethodPost, "/v1/orders", validBody, hdr)
	second := do(h, http.MethodPost, "/v1/orders", validBody, hdr)
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("statuses %d %d", first.Code, second.Code)
	}
	if second.Header().Get("Idempotent-Replayed") != "true" || !bytes.Equal(first.Body.Bytes(), second.Body.Bytes()) {
		t.Fatal("second response must be a byte-identical replay")
	}
	if svc.createN != 1 {
		t.Fatalf("handler must run once, ran %d times", svc.createN)
	}
	other := strings.Replace(validBody, `"quantity":2`, `"quantity":3`, 1)
	if rec := do(h, http.MethodPost, "/v1/orders", other, hdr); rec.Code != http.StatusUnprocessableEntity {
		t.Fatalf("same key with different payload must be 422, got %d", rec.Code)
	}
	// Without a key the request is not deduplicated.
	do(h, http.MethodPost, "/v1/orders", validBody, nil)
	if svc.createN != 2 {
		t.Fatalf("keyless request must execute, createN=%d", svc.createN)
	}
}

func TestIdempotencyDoesNotStoreServerErrors(t *testing.T) {
	t.Parallel()
	svc := &fakeSvc{order: newOrder(), createErr: errors.New("transient")}
	h := newRouter(svc, func(d *httpapi.Deps) { d.Idempotency = &memIdem{} })
	hdr := map[string]string{"Idempotency-Key": "k2"}
	if rec := do(h, http.MethodPost, "/v1/orders", validBody, hdr); rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rec.Code)
	}
	svc.createErr = nil
	if rec := do(h, http.MethodPost, "/v1/orders", validBody, hdr); rec.Code != http.StatusCreated || rec.Header().Get("Idempotent-Replayed") != "" {
		t.Fatalf("retry after a 5xx must re-execute, got %d", rec.Code)
	}
}

func TestRateLimit(t *testing.T) {
	t.Parallel()
	url := "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906"
	denied := newRouter(&fakeSvc{order: newOrder()}, func(d *httpapi.Deps) { d.Limiter = fakeLimiter{allowed: false} })
	rec := do(denied, http.MethodGet, url, "", nil)
	if rec.Code != http.StatusTooManyRequests || rec.Header().Get("Retry-After") != "2" {
		t.Fatalf("want 429 + Retry-After 2, got %d %q", rec.Code, rec.Header().Get("Retry-After"))
	}
	failOpen := newRouter(&fakeSvc{order: newOrder()}, func(d *httpapi.Deps) { d.Limiter = fakeLimiter{err: errors.New("redis down")} })
	if rec := do(failOpen, http.MethodGet, url, "", nil); rec.Code != http.StatusOK {
		t.Fatalf("limiter failure must fail open, got %d", rec.Code)
	}
}

func TestPanicIsRecovered(t *testing.T) {
	t.Parallel()
	h := newRouter(&fakeSvc{order: newOrder(), getPanic: true}, nil)
	if rec := do(h, http.MethodGet, "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906", "", nil); rec.Code != http.StatusInternalServerError {
		t.Fatalf("want 500, got %d", rec.Code)
	}
}

func TestRequestIDIsPropagated(t *testing.T) {
	t.Parallel()
	h := newRouter(&fakeSvc{order: newOrder()}, nil)
	rec := do(h, http.MethodGet, "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906", "", map[string]string{"X-Request-ID": "abc-123"})
	if got := rec.Header().Get("X-Request-Id"); got != "abc-123" {
		t.Fatalf("inbound request id must be reused, got %q", got)
	}
}

func BenchmarkGetOrder(b *testing.B) {
	h := newRouter(&fakeSvc{order: newOrder()}, nil)
	req := httptest.NewRequestWithContext(b.Context(), http.MethodGet, "/v1/orders/01a10d3e-b6c3-7975-ba38-9406719bb906", http.NoBody)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		h.ServeHTTP(httptest.NewRecorder(), req)
	}
}
