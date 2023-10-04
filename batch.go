package batcher

import (
	"context"
	"time"
)

type batch struct {
	storage   storage
	timeout   time.Duration
	batchSize int

	timer         *time.Timer
	ch            chan struct{}
	keyList       []string
	resultChanMap map[string]chan any
	errorChanMap  map[string]chan error
}

func (s *batcher) newBatch() *batch {
	b := &batch{
		storage:       s.storage,
		timeout:       s.timeout,
		batchSize:     s.batchSize,
		ch:            make(chan struct{}),
		keyList:       make([]string, 0, s.batchSize),
		resultChanMap: make(map[string]chan any),
		errorChanMap:  make(map[string]chan error),
	}
	return b
}

func (s *batch) runtime() {
	s.timer = time.NewTimer(s.timeout)
	<-s.timer.C

	s.process()
}

// addKeyToBatch adds key to batch and return true if batch is full
func (s *batch) addKeyToBatch(key string, resCh chan any, errCh chan error) bool {
	s.keyList = append(s.keyList, key)
	s.resultChanMap[key] = resCh
	s.errorChanMap[key] = errCh

	if len(s.keyList) == 1 {
		go s.runtime()
	}

	return len(s.keyList) == s.batchSize
}

func (s *batch) process() {
	close(s.ch)
	if len(s.keyList) == 0 {
		return
	}
	s.timer.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), batchProcessingTimeout)
	defer cancel()

	result, err := s.storage.Get(ctx, s.keyList)
	if err != nil {
		for _, ch := range s.errorChanMap {
			select {
			case ch <- err:
			default:
			}
		}
		return
	}

	for _, res := range result {
		key := s.storage.KeyByValue(res)
		resCh := s.resultChanMap[key]
		delete(s.resultChanMap, key)

		select {
		case resCh <- res:
		default:
		}
	}

	for key := range s.resultChanMap {
		errCh := s.errorChanMap[key]
		select {
		case errCh <- errNotFound:
		default:
		}
	}
}
