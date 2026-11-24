package caches

import (
	"context"
	"fmt"
	"sync"
)

type flight struct {
	done  chan struct{}
	value any
	err   error
}

// Group shares one in-flight load of a key among concurrent callers.
type Group struct {
	mu      sync.Mutex
	pending map[any]*flight
}

func (g *Group) Do(key any, load func() (any, error), wait bool) (any, bool, error) {
	return g.do(context.Background(), key, load, wait)
}

func (g *Group) do(ctx context.Context, key any, load func() (any, error), wait bool) (any, bool, error) {
	g.mu.Lock()
	if f := g.pending[key]; f != nil {
		g.mu.Unlock()
		if !wait {
			return nil, false, KeyNotFoundError
		}
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-f.done:
			return f.value, false, f.err
		}
	}
	if g.pending == nil {
		g.pending = make(map[any]*flight)
	}
	f := &flight{done: make(chan struct{})}
	g.pending[key] = f
	g.mu.Unlock()

	run := func() {
		defer func() {
			if p := recover(); p != nil {
				f.err = fmt.Errorf("cache loader panic: %v", p)
			}
			g.mu.Lock()
			delete(g.pending, key)
			close(f.done)
			g.mu.Unlock()
		}()
		f.value, f.err = load()
	}
	if !wait {
		go run()
		return nil, false, KeyNotFoundError
	}
	run()
	return f.value, true, f.err
}
