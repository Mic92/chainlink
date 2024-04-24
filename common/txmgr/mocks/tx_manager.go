// Code generated by mockery v2.42.2. DO NOT EDIT.

package mocks

import (
	context "context"
	big "math/big"

	feetypes "github.com/smartcontractkit/chainlink/v2/common/fee/types"
	mock "github.com/stretchr/testify/mock"

	null "gopkg.in/guregu/null.v4"

	txmgr "github.com/smartcontractkit/chainlink/v2/common/txmgr"

	txmgrtypes "github.com/smartcontractkit/chainlink/v2/common/txmgr/types"

	types "github.com/smartcontractkit/chainlink/v2/common/types"
)

// TxManager is an autogenerated mock type for the TxManager type
type TxManager[CHAIN_ID types.ID, HEAD types.Head[BLOCK_HASH], ADDR types.Hashable, TX_HASH types.Hashable, BLOCK_HASH types.Hashable, SEQ types.Sequence, FEE feetypes.Fee] struct {
	mock.Mock
}

// Close provides a mock function with given fields:
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Close() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Close")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// CountTransactionsByState provides a mock function with given fields: ctx, state
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) CountTransactionsByState(ctx context.Context, state txmgrtypes.TxState) (uint32, error) {
	ret := _m.Called(ctx, state)

	if len(ret) == 0 {
		panic("no return value specified for CountTransactionsByState")
	}

	var r0 uint32
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, txmgrtypes.TxState) (uint32, error)); ok {
		return rf(ctx, state)
	}
	if rf, ok := ret.Get(0).(func(context.Context, txmgrtypes.TxState) uint32); ok {
		r0 = rf(ctx, state)
	} else {
		r0 = ret.Get(0).(uint32)
	}

	if rf, ok := ret.Get(1).(func(context.Context, txmgrtypes.TxState) error); ok {
		r1 = rf(ctx, state)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// CreateTransaction provides a mock function with given fields: ctx, txRequest
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) CreateTransaction(ctx context.Context, txRequest txmgrtypes.TxRequest[ADDR, TX_HASH]) (txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, txRequest)

	if len(ret) == 0 {
		panic("no return value specified for CreateTransaction")
	}

	var r0 txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, txmgrtypes.TxRequest[ADDR, TX_HASH]) (txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, txRequest)
	}
	if rf, ok := ret.Get(0).(func(context.Context, txmgrtypes.TxRequest[ADDR, TX_HASH]) txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, txRequest)
	} else {
		r0 = ret.Get(0).(txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
	}

	if rf, ok := ret.Get(1).(func(context.Context, txmgrtypes.TxRequest[ADDR, TX_HASH]) error); ok {
		r1 = rf(ctx, txRequest)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindEarliestUnconfirmedBroadcastTime provides a mock function with given fields: ctx
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindEarliestUnconfirmedBroadcastTime(ctx context.Context) (null.Time, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for FindEarliestUnconfirmedBroadcastTime")
	}

	var r0 null.Time
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (null.Time, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) null.Time); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(null.Time)
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindEarliestUnconfirmedTxAttemptBlock provides a mock function with given fields: ctx
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindEarliestUnconfirmedTxAttemptBlock(ctx context.Context) (null.Int, error) {
	ret := _m.Called(ctx)

	if len(ret) == 0 {
		panic("no return value specified for FindEarliestUnconfirmedTxAttemptBlock")
	}

	var r0 null.Int
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context) (null.Int, error)); ok {
		return rf(ctx)
	}
	if rf, ok := ret.Get(0).(func(context.Context) null.Int); ok {
		r0 = rf(ctx)
	} else {
		r0 = ret.Get(0).(null.Int)
	}

	if rf, ok := ret.Get(1).(func(context.Context) error); ok {
		r1 = rf(ctx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindTxesByMetaFieldAndStates provides a mock function with given fields: ctx, metaField, metaValue, states, chainID
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindTxesByMetaFieldAndStates(ctx context.Context, metaField string, metaValue string, states []txmgrtypes.TxState, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, metaField, metaValue, states, chainID)

	if len(ret) == 0 {
		panic("no return value specified for FindTxesByMetaFieldAndStates")
	}

	var r0 []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []txmgrtypes.TxState, *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, metaField, metaValue, states, chainID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, string, []txmgrtypes.TxState, *big.Int) []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, metaField, metaValue, states, chainID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, string, []txmgrtypes.TxState, *big.Int) error); ok {
		r1 = rf(ctx, metaField, metaValue, states, chainID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindTxesWithAttemptsAndReceiptsByIdsAndState provides a mock function with given fields: ctx, ids, states, chainID
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindTxesWithAttemptsAndReceiptsByIdsAndState(ctx context.Context, ids []int64, states []txmgrtypes.TxState, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, ids, states, chainID)

	if len(ret) == 0 {
		panic("no return value specified for FindTxesWithAttemptsAndReceiptsByIdsAndState")
	}

	var r0 []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, []int64, []txmgrtypes.TxState, *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, ids, states, chainID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, []int64, []txmgrtypes.TxState, *big.Int) []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, ids, states, chainID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, []int64, []txmgrtypes.TxState, *big.Int) error); ok {
		r1 = rf(ctx, ids, states, chainID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindTxesWithMetaFieldByReceiptBlockNum provides a mock function with given fields: ctx, metaField, blockNum, chainID
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindTxesWithMetaFieldByReceiptBlockNum(ctx context.Context, metaField string, blockNum int64, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, metaField, blockNum, chainID)

	if len(ret) == 0 {
		panic("no return value specified for FindTxesWithMetaFieldByReceiptBlockNum")
	}

	var r0 []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, int64, *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, metaField, blockNum, chainID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, int64, *big.Int) []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, metaField, blockNum, chainID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, int64, *big.Int) error); ok {
		r1 = rf(ctx, metaField, blockNum, chainID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// FindTxesWithMetaFieldByStates provides a mock function with given fields: ctx, metaField, states, chainID
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) FindTxesWithMetaFieldByStates(ctx context.Context, metaField string, states []txmgrtypes.TxState, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, metaField, states, chainID)

	if len(ret) == 0 {
		panic("no return value specified for FindTxesWithMetaFieldByStates")
	}

	var r0 []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, string, []txmgrtypes.TxState, *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, metaField, states, chainID)
	}
	if rf, ok := ret.Get(0).(func(context.Context, string, []txmgrtypes.TxState, *big.Int) []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, metaField, states, chainID)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
		}
	}

	if rf, ok := ret.Get(1).(func(context.Context, string, []txmgrtypes.TxState, *big.Int) error); ok {
		r1 = rf(ctx, metaField, states, chainID)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// GetForwarderForEOA provides a mock function with given fields: eoa
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) GetForwarderForEOA(eoa ADDR) (ADDR, error) {
	ret := _m.Called(eoa)

	if len(ret) == 0 {
		panic("no return value specified for GetForwarderForEOA")
	}

	var r0 ADDR
	var r1 error
	if rf, ok := ret.Get(0).(func(ADDR) (ADDR, error)); ok {
		return rf(eoa)
	}
	if rf, ok := ret.Get(0).(func(ADDR) ADDR); ok {
		r0 = rf(eoa)
	} else {
		r0 = ret.Get(0).(ADDR)
	}

	if rf, ok := ret.Get(1).(func(ADDR) error); ok {
		r1 = rf(eoa)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// HealthReport provides a mock function with given fields:
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) HealthReport() map[string]error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for HealthReport")
	}

	var r0 map[string]error
	if rf, ok := ret.Get(0).(func() map[string]error); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(map[string]error)
		}
	}

	return r0
}

// Name provides a mock function with given fields:
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Name() string {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Name")
	}

	var r0 string
	if rf, ok := ret.Get(0).(func() string); ok {
		r0 = rf()
	} else {
		r0 = ret.Get(0).(string)
	}

	return r0
}

// OnNewLongestChain provides a mock function with given fields: ctx, head
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) OnNewLongestChain(ctx context.Context, head HEAD) {
	_m.Called(ctx, head)
}

// Ready provides a mock function with given fields:
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Ready() error {
	ret := _m.Called()

	if len(ret) == 0 {
		panic("no return value specified for Ready")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func() error); ok {
		r0 = rf()
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// RegisterResumeCallback provides a mock function with given fields: fn
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) RegisterResumeCallback(fn txmgr.ResumeCallback) {
	_m.Called(fn)
}

// Reset provides a mock function with given fields: addr, abandon
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Reset(addr ADDR, abandon bool) error {
	ret := _m.Called(addr, abandon)

	if len(ret) == 0 {
		panic("no return value specified for Reset")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(ADDR, bool) error); ok {
		r0 = rf(addr, abandon)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// SendNativeToken provides a mock function with given fields: ctx, chainID, from, to, value, gasLimit
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) SendNativeToken(ctx context.Context, chainID CHAIN_ID, from ADDR, to ADDR, value big.Int, gasLimit uint64) (txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ret := _m.Called(ctx, chainID, from, to, value, gasLimit)

	if len(ret) == 0 {
		panic("no return value specified for SendNativeToken")
	}

	var r0 txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	var r1 error
	if rf, ok := ret.Get(0).(func(context.Context, CHAIN_ID, ADDR, ADDR, big.Int, uint64) (txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error)); ok {
		return rf(ctx, chainID, from, to, value, gasLimit)
	}
	if rf, ok := ret.Get(0).(func(context.Context, CHAIN_ID, ADDR, ADDR, big.Int, uint64) txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]); ok {
		r0 = rf(ctx, chainID, from, to, value, gasLimit)
	} else {
		r0 = ret.Get(0).(txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE])
	}

	if rf, ok := ret.Get(1).(func(context.Context, CHAIN_ID, ADDR, ADDR, big.Int, uint64) error); ok {
		r1 = rf(ctx, chainID, from, to, value, gasLimit)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Start provides a mock function with given fields: _a0
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Start(_a0 context.Context) error {
	ret := _m.Called(_a0)

	if len(ret) == 0 {
		panic("no return value specified for Start")
	}

	var r0 error
	if rf, ok := ret.Get(0).(func(context.Context) error); ok {
		r0 = rf(_a0)
	} else {
		r0 = ret.Error(0)
	}

	return r0
}

// Trigger provides a mock function with given fields: addr
func (_m *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) Trigger(addr ADDR) {
	_m.Called(addr)
}

// NewTxManager creates a new instance of TxManager. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewTxManager[CHAIN_ID types.ID, HEAD types.Head[BLOCK_HASH], ADDR types.Hashable, TX_HASH types.Hashable, BLOCK_HASH types.Hashable, SEQ types.Sequence, FEE feetypes.Fee](t interface {
	mock.TestingT
	Cleanup(func())
}) *TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	mock := &TxManager[CHAIN_ID, HEAD, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
