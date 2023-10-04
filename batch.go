package batcher

import (
	"context"
)

type batch struct {
	storage storage

	batchSize      int
	keyList        []string
	resultChanList []chan any
	errorChanList  []chan error
}

func (s *batcher) newBatch() *batch {
	b := &batch{
		storage:   s.storage,
		batchSize: s.batchSize,
		keyList:   make([]string, s.batchSize),
	}
	return b
}

// addKeyToBatch adds key to batch and return true if batch is full
func (s *batch) addKeyToBatch(key string, resCh chan any, errCh chan error) bool {
	s.keyList = append(s.keyList, key)
	s.resultChanList = append(s.resultChanList, resCh)
	s.errorChanList = append(s.errorChanList, errCh)

	return len(s.keyList) == s.batchSize
}

func (s *batch) process(ctx context.Context) {
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
