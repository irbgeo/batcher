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

type storage interface {
	// необходимая условность: получаемые данные простортированы в соответсвии с порядком ключей
	Get(ctx context.Context, keys []string) ([]any, error)
}

func NewBatcher(
	batchSize int,
	timeout time.Duration,
	s storage,
) *batcher {
	b := &batcher{
		batchSize: batchSize,
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

		go func(b *batch) {
			ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
			defer cancel()

			b.process(ctx)
		}(s.batch)

		s.batch = s.newBatch()

		s.mu.Unlock()
	}
}

func (s *batcher) Close() {
	s.ticker.Stop()
}

func (s *batcher) AddKey(ctx context.Context, key string) (any, error) {
	resCh := make(chan any)
	errCh := make(chan error)

	s.mu.Lock()
	if err := s.batch.addKeyToBatch(key, resCh, errCh); err != nil {
		go func(b *batch) {
			ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
			defer cancel()

			b.process(ctx)
		}(s.batch)

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
