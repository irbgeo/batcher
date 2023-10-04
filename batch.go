package batcher

import (
	"context"
	"time"
)

type batch struct {
	storage   storage
	timeout   time.Duration
	batchSize int

	timer          *time.Timer
	ch             chan struct{}
	keyList        []string
	resultChanList []chan any
	errorChanList  []chan error
}

func (s *batcher) newBatch() *batch {
	b := &batch{
		storage:   s.storage,
		timeout:   s.timeout,
		batchSize: s.batchSize,
		ch:        make(chan struct{}),
		keyList:   make([]string, 0, s.batchSize),
	}
	return b
}

func (s *batch) runtime() {
	s.timer = time.NewTimer(s.timeout)
	<-s.timer.C

	ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
	defer cancel()

	s.process(ctx)
}

// addKeyToBatch adds key to batch and return true if batch is full
func (s *batch) addKeyToBatch(key string, resCh chan any, errCh chan error) bool {
	s.keyList = append(s.keyList, key)
	s.resultChanList = append(s.resultChanList, resCh)
	s.errorChanList = append(s.errorChanList, errCh)

	if len(s.keyList) == 1 {
		go s.runtime()
	}

	return len(s.keyList) == s.batchSize
}

func (s *batch) process(ctx context.Context) {
	close(s.ch)
	if len(s.keyList) == 0 {
		return
	}
	s.timer.Stop()

	result, err := s.storage.Get(ctx, s.keyList)
	if err != nil {
		for _, ch := range s.errorChanList {
			select {
			case ch <- err:
			default:
			}
		}
		return
	}

	for i, res := range result {
		select {
		case s.resultChanList[i] <- res:
		default:
		}
	}
}
