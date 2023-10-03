package batcher

import (
	"context"
)

type batch struct {
	storage storage

	keyCh          chan string
	resultChanList []chan any
	errorChanList  []chan error
}

func (s *batcher) newBatch() *batch {
	b := &batch{
		storage: s.storage,
		keyCh:   make(chan string, s.batchSize),
	}
	return b
}

func (s *batch) addKeyToBatch(key string, resCh chan any, errCh chan error) bool {
	select {
	case s.keyCh <- key:
		s.resultChanList = append(s.resultChanList, resCh)
		s.errorChanList = append(s.errorChanList, errCh)
	default:
		return false
	}

	return true
}

func (s *batch) process(ctx context.Context) {
	keys := make([]string, 0, len(s.keyCh))
	for len(s.keyCh) > 0 {
		keys = append(keys, <-s.keyCh)
	}

	result, err := s.storage.Get(ctx, keys)
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
