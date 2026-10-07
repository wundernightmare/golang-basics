package api_test

import (
	"context"
	"sort"
	"sync"
	"time"

	"github.com/tracehubmmp/golang-basics/services/tasks/internal/domain"
)

// fakeStore is an in-memory api.Store with the real one's semantics:
// versions, If-Match, idempotency keys, keyset pages. Events are the real
// store's business (tested against Postgres in internal/store).
type fakeStore struct {
	mu       sync.Mutex
	tasks    map[string]domain.Task
	idem     map[string]domain.IdempotencyKey // key → hash
	idemTask map[string]string                // key → task id
	clock    time.Time
	err      error // returned by every call when set
	getCalls int
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		tasks: map[string]domain.Task{}, idem: map[string]domain.IdempotencyKey{}, idemTask: map[string]string{},
		clock: time.Date(2026, 9, 25, 12, 0, 0, 0, time.UTC),
	}
}

func (f *fakeStore) put(t domain.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if t.Version == 0 {
		t.Version = 1
	}
	f.tasks[t.ID] = t
}

func (f *fakeStore) snapshot() map[string]domain.Task {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]domain.Task, len(f.tasks))
	for k, v := range f.tasks {
		out[k] = v
	}
	return out
}

func (f *fakeStore) Create(_ context.Context, id, title string, idem *domain.IdempotencyKey) (domain.Task, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Task{}, false, f.err
	}
	if idem != nil {
		if prev, ok := f.idem[idem.Key]; ok {
			if prev.Hash != idem.Hash {
				return domain.Task{}, false, domain.ErrIdempotencyKeyReused
			}
			return f.tasks[f.idemTask[idem.Key]], true, nil
		}
		f.idem[idem.Key] = *idem
		f.idemTask[idem.Key] = id
	}
	f.clock = f.clock.Add(time.Second)
	t := domain.Task{ID: id, Title: title, Version: 1, CreatedAt: f.clock}
	f.tasks[id] = t
	return t, false, nil
}

func (f *fakeStore) Get(_ context.Context, id string) (domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.getCalls++
	if f.err != nil {
		return domain.Task{}, f.err
	}
	t, ok := f.tasks[id]
	if !ok {
		return domain.Task{}, domain.ErrNotFound
	}
	return t, nil
}

func (f *fakeStore) List(_ context.Context, req domain.PageRequest) (domain.Page, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Page{}, f.err
	}
	all := make([]domain.Task, 0, len(f.tasks))
	for _, t := range f.tasks {
		all = append(all, t)
	}
	sort.Slice(all, func(i, j int) bool {
		if !all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].CreatedAt.After(all[j].CreatedAt)
		}
		return all[i].ID > all[j].ID
	})
	var page []domain.Task
	for _, t := range all {
		if c := req.Cursor; c != nil && !after(t, *c) {
			continue
		}
		page = append(page, t)
	}
	out := domain.Page{Tasks: page}
	if len(page) > req.Limit {
		out.Tasks = page[:req.Limit]
		next := domain.CursorAfter(out.Tasks[req.Limit-1])
		out.Next = &next
	}
	return out, nil
}

// after reports whether t comes after c in (created_at DESC, id DESC) order.
func after(t domain.Task, c domain.Cursor) bool {
	if t.CreatedAt.Equal(c.CreatedAt) {
		return t.ID < c.ID
	}
	return t.CreatedAt.Before(c.CreatedAt)
}

func (f *fakeStore) Update(_ context.Context, id string, p domain.Patch, expect *int64) (domain.Task, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return domain.Task{}, f.err
	}
	t, ok := f.tasks[id]
	switch {
	case !ok:
		return domain.Task{}, domain.ErrNotFound
	case expect != nil && *expect != t.Version:
		return domain.Task{}, domain.ErrVersionMismatch
	}
	t = p.Apply(t)
	t.Version++
	f.tasks[id] = t
	return t, nil
}

func (f *fakeStore) Delete(_ context.Context, id string, expect *int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	t, ok := f.tasks[id]
	switch {
	case !ok:
		return domain.ErrNotFound
	case expect != nil && *expect != t.Version:
		return domain.ErrVersionMismatch
	}
	delete(f.tasks, id)
	return nil
}

// fakeCache is an in-memory api.Cache recording what the handlers did.
type fakeCache struct {
	mu          sync.Mutex
	data        map[string]domain.Task
	invalidated []string
	invErr      error
}

func newFakeCache() *fakeCache { return &fakeCache{data: map[string]domain.Task{}} }

func (f *fakeCache) Get(ctx context.Context, id string, load func(context.Context) (domain.Task, error)) (domain.Task, bool, error) {
	f.mu.Lock()
	t, ok := f.data[id]
	f.mu.Unlock()
	if ok {
		return t, true, nil
	}
	t, err := load(ctx)
	if err != nil {
		return domain.Task{}, false, err
	}
	f.mu.Lock()
	f.data[id] = t
	f.mu.Unlock()
	return t, false, nil
}

func (f *fakeCache) Warm(_ context.Context, t domain.Task) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[t.ID] = t
}

func (f *fakeCache) Invalidate(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.invalidated = append(f.invalidated, id)
	delete(f.data, id)
	return f.invErr
}

func (f *fakeCache) has(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.data[id]
	return ok
}
