package batcher

import (
	"context"
	"sync"
	"time"
)

type batcher struct {
	batchSize int
	timeout   time.Duration

	ticker  *time.Ticker
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
		ticker:    time.NewTicker(timeout),
		storage:   s,
	}

	b.batch = b.newBatch()

	go b.runtime()
	return b
}

func (s *batcher) runtime() {
	for range s.ticker.C {
		s.mu.Lock()

		go s.processBatch(s.batch)

		s.ticker.Reset(s.timeout)
		s.batch = s.newBatch()

		s.mu.Unlock()
	}
}

func (s *batcher) Close(ctx context.Context) {
	s.processBatch(s.batch)

	s.ticker.Stop()
}

// AddKey to batch and return result or error
func (s *batcher) AddKey(ctx context.Context, key string) (any, error) {
	resCh := make(chan any)
	errCh := make(chan error)

	s.mu.Lock()
	if !s.batch.addKeyToBatch(key, resCh, errCh) {
		go s.processBatch(s.batch)

		s.batch = s.newBatch()
		s.batch.addKeyToBatch(key, resCh, errCh) // nolint: errcheck
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

func (s *batcher) processBatch(b *batch) {
	ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
	defer cancel()

	b.process(ctx)
}
