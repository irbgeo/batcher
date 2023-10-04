package batcher

import (
	"context"
	"sync"
	"time"
)

type batcher struct {
	batchSize int
	timeout   time.Duration

	ch      chan struct{}
	storage storage
	mu      sync.Mutex
	batch   *batch
}

//go:generate mockery --name=storage --structname=Storage
type storage interface {
	// The necessary condition is that the received data is sorted according to the order of keys.
	Get(ctx context.Context, keys []string) ([]any, error)
}

func New(
	batchSize int,
	timeout time.Duration,
	s storage,
) *batcher {
	b := &batcher{
		batchSize: batchSize,
		timeout:   timeout,
		storage:   s,
		ch:        make(chan struct{}),
	}

	b.batch = b.newBatch()

	go b.waitBatchClose(b.batch)
	return b
}

func (s *batcher) waitBatchClose(b *batch) {
	select {
	case <-b.ch:
		s.batch = s.newBatch()
		go s.waitBatchClose(s.batch)
	case <-s.ch:
	}
}

func (s *batcher) Close(ctx context.Context) {
	s.batch.process()
	close(s.ch)
}

// AddKey to batch and return result or error
func (s *batcher) AddKey(ctx context.Context, key string) (any, error) {
	resCh := make(chan any)
	errCh := make(chan error)

	s.mu.Lock()
	if s.batch.addKeyToBatch(key, resCh, errCh) {
		go s.batch.process()
	}
	s.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-resCh:
		return res, nil
	case err := <-errCh:
		return nil, err
	}
}
