package batcher

import (
	"container/list"
	"context"
)

type batch struct {
	storage storage

	keyCh          chan string
	resultChanList *list.List
	errorChanList  *list.List
}

func (s *batcher) newBatch() *batch {
	b := &batch{
		storage:        s.storage,
		keyCh:          make(chan string, s.batchSize),
		resultChanList: list.New(),
		errorChanList:  list.New(),
	}

	s.ticker.Reset(s.timeout)
	return b
}

func (s *batch) addKeyToBatch(key string, resCh <-chan any, errCh <-chan error) error {
	select {
	case s.keyCh <- key:
		s.resultChanList.PushBack(resCh)
		s.errorChanList.PushBack(errCh)
	default:
		return errBatchIsFull
	}

	return nil
}

func (s *batch) process(ctx context.Context) {
	if len(s.keyCh) == 0 {
		return
	}

	keys := make([]string, 0, len(s.keyCh))
	for len(s.keyCh) > 0 {
		keys = append(keys, <-s.keyCh)
	}

	result, err := s.storage.Get(ctx, keys)
	if err != nil {
		for e := s.errorChanList.Front(); e != nil; e = e.Next() {
			select {
			case e.Value.(chan error) <- err:
			default:
			}
		}
		return
	}

	for _, res := range result {
		select {
		case s.resultChanList.Front().Value.(chan any) <- res:
		default:
		}
		s.resultChanList.Remove(s.resultChanList.Front())
	}
}
