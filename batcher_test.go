package batcher_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/irbgeo/batcher"
	"github.com/irbgeo/batcher/mocks"
)

type testCase struct {
	key           string
	expectedValue any
	expectedError error
}

var (
	errStorageError = errors.New("storage error")
	testTimeout     = 10 * time.Second

	testSet = []testCase{
		{
			key:           "test-case-1",
			expectedValue: "test-actualValue-1",
			expectedError: errStorageError,
		},
		{
			key:           "test-case-2",
			expectedValue: "test-actualValue-2",
			expectedError: errStorageError,
		},
		{
			key:           "test-case-3",
			expectedValue: "test-actualValue-3",
			expectedError: errStorageError,
		},
		{
			key:           "test-case-4",
			expectedValue: "test-actualValue-4",
			expectedError: errStorageError,
		},
	}
)

func TestBatch(t *testing.T) {
	keys := make([]string, 0, len(testSet))
	values := make([]any, 0, len(testSet))
	for _, c := range testSet {
		keys = append(keys, c.key)
		values = append(values, c.expectedValue)
	}
	storageMock := mocks.NewStorage(t)
	storageMock.On("Get", mock.Anything, keys).Return(values, nil)

	b := batcher.New(
		len(testSet),
		testTimeout,
		storageMock,
	)
	defer b.Close(context.Background())

	wg := &sync.WaitGroup{}
	for _, c := range testSet {
		wg.Add(1)
		go func(c testCase) {
			defer wg.Done()

			actualValue, err := b.AddKey(context.Background(), c.key)
			require.NoError(t, err)
			require.Equal(t, c.expectedValue, actualValue)
		}(c)

		<-time.After(time.Second)
	}

	wg.Wait()
}

func TestNotFullBatch(t *testing.T) {
	finishIdx := len(testSet) / 2

	keys := make([]string, 0, len(testSet))
	values := make([]any, 0, len(testSet))
	for i, c := range testSet {
		if i == finishIdx {
			break
		}
		keys = append(keys, c.key)
		values = append(values, c.expectedValue)
	}
	storageMock := mocks.NewStorage(t)
	storageMock.On("Get", mock.Anything, keys).Return(values, nil)

	b := batcher.New(
		len(testSet),
		testTimeout,
		storageMock,
	)

	wg := &sync.WaitGroup{}

	for i, c := range testSet {
		if i == finishIdx {
			break
		}

		wg.Add(1)
		go func(c testCase) {
			defer wg.Done()

			actualValue, err := b.AddKey(context.Background(), c.key)
			require.NoError(t, err)
			require.Equal(t, c.expectedValue, actualValue)
		}(c)
		<-time.After(time.Second)
	}

	wg.Wait()
}

func TestClose(t *testing.T) {
	finishIdx := len(testSet) / 2

	keys := make([]string, 0, len(testSet))
	values := make([]any, 0, len(testSet))
	for i, c := range testSet {
		if i == finishIdx {
			break
		}
		keys = append(keys, c.key)
		values = append(values, c.expectedValue)
	}
	storageMock := mocks.NewStorage(t)
	storageMock.On("Get", mock.Anything, keys).Return(values, nil)

	b := batcher.New(
		len(testSet),
		testTimeout,
		storageMock,
	)

	wg := &sync.WaitGroup{}

	for i, c := range testSet {
		if i == finishIdx {
			b.Close(context.Background())
			break
		}

		wg.Add(1)
		go func(c testCase) {
			defer wg.Done()

			actualValue, err := b.AddKey(context.Background(), c.key)
			require.NoError(t, err)
			require.Equal(t, c.expectedValue, actualValue)
		}(c)
		<-time.After(time.Second)
	}

	wg.Wait()
}

func TestSeveralBatch(t *testing.T) {
	keys := make([]string, 0, len(testSet))
	values := make([]any, 0, len(testSet))
	for _, c := range testSet {
		keys = append(keys, c.key)
		values = append(values, c.expectedValue)
	}
	storageMock := mocks.NewStorage(t)
	storageMock.On("Get", mock.Anything, keys).Return(values, nil)

	b := batcher.New(
		len(testSet),
		testTimeout,
		storageMock,
	)
	defer b.Close(context.Background())

	wg := &sync.WaitGroup{}
	for i := 0; i < 3; i++ {
		for _, c := range testSet {
			wg.Add(1)
			go func(c testCase) {
				defer wg.Done()

				actualValue, err := b.AddKey(context.Background(), c.key)
				require.NoError(t, err)
				require.Equal(t, c.expectedValue, actualValue)
			}(c)

			<-time.After(time.Second)
		}
	}
	wg.Wait()
}

func TestFailed(t *testing.T) {
	keys := make([]string, 0, len(testSet))
	for _, c := range testSet {
		keys = append(keys, c.key)
	}
	storageMock := mocks.NewStorage(t)
	storageMock.On("Get", mock.Anything, keys).Return(nil, errStorageError)

	b := batcher.New(
		len(testSet),
		testTimeout,
		storageMock,
	)
	defer b.Close(context.Background())

	wg := &sync.WaitGroup{}

	for _, c := range testSet {
		wg.Add(1)
		go func(c testCase) {
			defer wg.Done()

			_, err := b.AddKey(context.Background(), c.key)
			require.Equal(t, c.expectedError, err)
		}(c)

		<-time.After(time.Second)
	}

	wg.Wait()
}
