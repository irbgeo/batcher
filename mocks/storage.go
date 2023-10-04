// Code generated by mockery v2.20.0. DO NOT EDIT.

package mocks

import (
	context "context"

	mock "github.com/stretchr/testify/mock"
)

// Storage is an autogenerated mock type for the storage type
type Storage struct {
	mock.Mock
}

// Get provides a mock function with given fields: ctx, keys
func (_m *Storage) Get(ctx context.Context, keys []string) ([]interface{}, error) {
	ret := _m.Called(ctx, keys)

	var r0 []interface{}
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []string) ([]interface{}, error)); ok {
		return rf(ctx, keys)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []string) []interface{}); ok {
		r0 = rf(ctx, keys)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]interface{})
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []string) error); ok {
		r1 = rf(ctx, keys)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// KeyByValue provides a mock function with given fields: v
func (_m *Storage) KeyByValue(v interface{}) string {
	ret := _m.Called(v)

	var r0 string
	if rf, ok := ret.Get(0).(func(interface{}) string); ok {
		r0 = rf(v)
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

type mockConstructorTestingTNewStorage interface {
	mock.TestingT
	Cleanup(func())
}

// NewStorage creates a new instance of Storage. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewStorage(t mockConstructorTestingTNewStorage) *Storage {
	mock := &Storage{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
