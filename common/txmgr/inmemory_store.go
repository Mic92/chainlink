package txmgr

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/smartcontractkit/chainlink-common/pkg/logger"
	feetypes "github.com/smartcontractkit/chainlink/v2/common/fee/types"
	txmgrtypes "github.com/smartcontractkit/chainlink/v2/common/txmgr/types"
	"github.com/smartcontractkit/chainlink/v2/common/types"
	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/label"
	"gopkg.in/guregu/null.v4"
)

var (
	// ErrInvalidChainID is returned when the chain ID is invalid
	ErrInvalidChainID = errors.New("invalid chain ID")
	// ErrTxnNotFound is returned when a transaction is not found
	ErrTxnNotFound = errors.New("transaction not found")
	// ErrExistingIdempotencyKey is returned when a transaction with the same idempotency key already exists
	ErrExistingIdempotencyKey = errors.New("transaction with idempotency key already exists")
	// ErrAddressNotFound is returned when an address is not found
	ErrAddressNotFound = errors.New("address not found")
	// ErrSequenceNotFound is returned when a sequence is not found
	ErrSequenceNotFound = errors.New("sequence not found")
	// ErrCouldNotGetReceipt is the error string we save if we reach our finality depth for a confirmed transaction without ever getting a receipt
	// This most likely happened because an external wallet used the account for this nonce
	ErrCouldNotGetReceipt = errors.New("could not get receipt")
)

type InMemoryStore[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
] struct {
	lggr    logger.SugaredLogger
	chainID CHAIN_ID

	keyStore txmgrtypes.KeyStore[ADDR, CHAIN_ID, SEQ]
	txStore  txmgrtypes.TxStore[ADDR, CHAIN_ID, TX_HASH, BLOCK_HASH, R, SEQ, FEE]

	addressStatesLock sync.RWMutex
	addressStates     map[ADDR]*AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]
}

// NewInMemoryStore returns a new InMemoryStore
func NewInMemoryStore[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
](
	ctx context.Context,
	lggr logger.SugaredLogger,
	chainID CHAIN_ID,
	keyStore txmgrtypes.KeyStore[ADDR, CHAIN_ID, SEQ],
	txStore txmgrtypes.TxStore[ADDR, CHAIN_ID, TX_HASH, BLOCK_HASH, R, SEQ, FEE],
) (*InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE], error) {
	ms := InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]{
		lggr:     lggr,
		chainID:  chainID,
		keyStore: keyStore,
		txStore:  txStore,

		addressStates: map[ADDR]*AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]{},
	}

	maxUnstarted := 50
	addresses, err := keyStore.EnabledAddressesForChain(chainID)
	if err != nil {
		return nil, fmt.Errorf("new_in_memory_store: %w", err)
	}
	for _, fromAddr := range addresses {
		txs, err := txStore.AllTransactions(ctx, fromAddr, chainID)
		if err != nil {
			return nil, fmt.Errorf("address_state: initialization: %w", err)
		}
		as, err := NewAddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE](lggr, chainID, fromAddr, maxUnstarted, txs)
		if err != nil {
			return nil, fmt.Errorf("new_in_memory_store: %w", err)
		}

		ms.addressStates[fromAddr] = as
	}

	return &ms, nil
}

// CreateTransaction creates a new transaction for a given txRequest.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CreateTransaction(
	ctx context.Context,
	txRequest txmgrtypes.TxRequest[ADDR, TX_HASH],
	chainID CHAIN_ID,
) (
	tx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	err error,
) {
	if ms.chainID.String() != chainID.String() {
		return tx, fmt.Errorf("create_transaction: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[tx.FromAddress]
	if !ok {
		return tx, fmt.Errorf("create_transaction: %w", ErrAddressNotFound)
	}

	// Persist Transaction to persistent storage
	tx, err = ms.txStore.CreateTransaction(ctx, txRequest, chainID)
	if err != nil {
		return tx, fmt.Errorf("create_transaction: %w", err)
	}

	// Update in memory store
	// Add the request to the Unstarted channel to be processed by the Broadcaster
	if err := as.AddTxToUnstarted(&tx); err != nil {
		return *ms.deepCopyTx(tx), fmt.Errorf("create_transaction: %w", err)
	}

	return *ms.deepCopyTx(tx), nil
}

// FindTxWithIdempotencyKey returns a transaction with the given idempotency key
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxWithIdempotencyKey(ctx context.Context, idempotencyKey string, chainID CHAIN_ID) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_tx_with_idempotency_key: %w", ErrInvalidChainID)
	}

	// Check if the transaction is in the pending queue of all address states
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		if tx := as.FindTxWithIdempotencyKey(idempotencyKey); tx != nil {
			return ms.deepCopyTx(*tx), nil
		}
	}

	return nil, fmt.Errorf("find_tx_with_idempotency_key: %w", ErrTxnNotFound)

}

// CheckTxQueueCapacity checks if the queue capacity has been reached for a given address
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CheckTxQueueCapacity(ctx context.Context, fromAddress ADDR, maxQueuedTransactions uint64, chainID CHAIN_ID) error {
	if maxQueuedTransactions == 0 {
		return nil
	}
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("check_tx_queue_capacity: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return fmt.Errorf("check_tx_queue_capacity: %w", ErrAddressNotFound)
	}

	count := uint64(as.CountTransactionsByState(TxUnstarted))
	if count >= maxQueuedTransactions {
		return fmt.Errorf("check_tx_queue_capacity: cannot create transaction; too many unstarted transactions in the queue (%v/%v). %s", count, maxQueuedTransactions, label.MaxQueuedTransactionsWarning)
	}

	return nil
}

// FindLatestSequence returns the latest sequence number for a given address
// It is used to initialize the in-memory sequence map in the broadcaster
// TODO(jtw): this is until we have a abstracted Sequencer Component which can be used instead
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindLatestSequence(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (SEQ, error) {
	// Query the persistent store
	return ms.txStore.FindLatestSequence(ctx, fromAddress, chainID)
}

// CountUnconfirmedTransactions returns the number of unconfirmed transactions for a given address.
// Unconfirmed transactions are transactions that have been broadcast but not confirmed on-chain.
// NOTE(jtw): used to calculate total inflight transactions
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountUnconfirmedTransactions(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (uint32, error) {
	if ms.chainID.String() != chainID.String() {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrAddressNotFound)
	}

	return uint32(as.CountTransactionsByState(TxUnconfirmed)), nil
}

// CountUnstartedTransactions returns the number of unstarted transactions for a given address.
// Unstarted transactions are transactions that have not been broadcast yet.
// NOTE(jtw): used to calculate total inflight transactions
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountUnstartedTransactions(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (uint32, error) {
	if ms.chainID.String() != chainID.String() {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrAddressNotFound)
	}

	return uint32(as.CountTransactionsByState(TxUnstarted)), nil
}

// UpdateTxUnstartedToInProgress updates a transaction from unstarted to in_progress.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxUnstartedToInProgress(
	ctx context.Context,
	tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	attempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) error {
	if tx.Sequence == nil {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: in_progress transaction must have a sequence number")
	}
	if tx.State != TxUnstarted {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: can only transition to in_progress from unstarted, transaction is currently %s", tx.State)
	}
	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: attempt state must be in_progress")
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[tx.FromAddress]
	if !ok {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxUnstartedToInProgress(ctx, tx, attempt); err != nil {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: %w", err)
	}

	// Update in address state in memory
	if err := as.MoveUnstartedToInProgress(tx, attempt); err != nil {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: %w", err)
	}

	return nil
}

// GetTxInProgress returns the in_progress transaction for a given address.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) GetTxInProgress(ctx context.Context, fromAddress ADDR) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return nil, fmt.Errorf("get_tx_in_progress: %w", ErrAddressNotFound)
	}

	tx, err := as.PeekInProgressTx()
	if tx == nil {
		return nil, fmt.Errorf("get_tx_in_progress: %w", err)
	}

	if len(tx.TxAttempts) != 1 || tx.TxAttempts[0].State != txmgrtypes.TxAttemptInProgress {
		return nil, fmt.Errorf("get_tx_in_progress: invariant violation: expected in_progress transaction %v to have exactly one unsent attempt. "+
			"Your database is in an inconsistent state and this node will not function correctly until the problem is resolved", tx.ID)
	}

	return ms.deepCopyTx(*tx), nil
}

// UpdateTxAttemptInProgressToBroadcast updates a transaction attempt from in_progress to broadcast.
// It also updates the transaction state to unconfirmed.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxAttemptInProgressToBroadcast(
	ctx context.Context,
	tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	newAttemptState txmgrtypes.TxAttemptState,
) error {
	if tx.BroadcastAt == nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: unconfirmed transaction must have broadcast_at time")
	}
	if tx.InitialBroadcastAt == nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: unconfirmed transaction must have initial_broadcast_at time")
	}
	if tx.State != TxInProgress {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: can only transition to unconfirmed from in_progress, transaction is currently %s", tx.State)
	}
	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: attempt must be in in_progress state")
	}
	if newAttemptState != txmgrtypes.TxAttemptBroadcast {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: new attempt state must be broadcast, got: %s", newAttemptState)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[tx.FromAddress]
	if !ok {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxAttemptInProgressToBroadcast(ctx, tx, attempt, newAttemptState); err != nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: %w", err)
	}

	// Update in memory store
	if err := as.MoveInProgressToUnconfirmed(*tx, attempt); err != nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: %w", err)
	}

	return nil
}

// FindNextUnstartedTransactionFromAddress returns the next unstarted transaction for a given address.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindNextUnstartedTransactionFromAddress(_ context.Context, tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], fromAddress ADDR, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: %w", ErrInvalidChainID)
	}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: %w", ErrAddressNotFound)
	}

	// ensure that the address is not already busy with a transaction in progress
	if as.inprogress != nil {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: address %s is already busy with a transaction in progress", fromAddress)
	}

	etx, err := as.PeekNextUnstartedTx()
	if err != nil || etx == nil {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: %w", err)
	}
	tx = ms.deepCopyTx(*etx)

	return nil
}

// SaveReplacementInProgressAttempt saves a replacement attempt for a transaction that is in_progress.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveReplacementInProgressAttempt(
	ctx context.Context,
	oldAttempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	replacementAttempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) error {
	if oldAttempt.State != txmgrtypes.TxAttemptInProgress || replacementAttempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("save_replacement_in_progress_attempt: expected attempts to be in_progress")
	}
	if oldAttempt.ID == 0 {
		return fmt.Errorf("save_replacement_in_progress_attempt: expected oldattempt to have an ID")
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[oldAttempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("save_replacement_in_progress_attempt: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveReplacementInProgressAttempt(ctx, oldAttempt, replacementAttempt); err != nil {
		return fmt.Errorf("save_replacement_in_progress_attempt: %w", err)
	}

	// Update in memory store
	tx, err := as.PeekInProgressTx()
	if tx == nil {
		return fmt.Errorf("save_replacement_in_progress_attempt: %w", err)
	}

	var found bool
	for i := 0; i < len(tx.TxAttempts); i++ {
		if tx.TxAttempts[i].ID == oldAttempt.ID {
			tx.TxAttempts[i] = *replacementAttempt
			found = true
		}
	}
	if !found {
		tx.TxAttempts = append(tx.TxAttempts, *replacementAttempt)
	}

	return nil
}

// UpdateTxFatalError updates a transaction to fatal_error.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxFatalError(ctx context.Context, tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	if tx.State != TxInProgress && tx.State != TxUnstarted {
		return fmt.Errorf("update_tx_fatal_error: can only transition to fatal_error from in_progress, transaction is currently %s", tx.State)
	}
	if !tx.Error.Valid {
		return fmt.Errorf("update_tx_fatal_error: expected error field to be set")
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[tx.FromAddress]
	if !ok {
		return fmt.Errorf("update_tx_fatal_error: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxFatalError(ctx, tx); err != nil {
		return fmt.Errorf("update_tx_fatal_error: %w", err)
	}

	// Update in memory store
	switch tx.State {
	case TxInProgress:
		if err := as.MoveInProgressToFatalError(tx.Error); err != nil {
			return fmt.Errorf("update_tx_fatal_error: %w", err)
		}
	case TxUnstarted:
		if err := as.MoveUnstartedToFatalError(*tx, tx.Error); err != nil {
			return fmt.Errorf("update_tx_fatal_error: %w", err)
		}
	}

	return nil
}

// Close closes the InMemoryStore
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) Close() {
	// Close the event recorder
	ms.txStore.Close()

	// Clear all address states
	ms.addressStatesLock.Lock()
	for _, as := range ms.addressStates {
		as.close()
	}
	clear(ms.addressStates)
	ms.addressStatesLock.Unlock()
}

// Abandon removes all transactions for a given address
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) Abandon(ctx context.Context, chainID CHAIN_ID, addr ADDR) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("abandon: %w", ErrInvalidChainID)
	}

	// Mark all persisted transactions as abandoned
	if err := ms.txStore.Abandon(ctx, chainID, addr); err != nil {
		return err
	}

	// check that the address exists in the unstarted transactions
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[addr]
	if !ok {
		return fmt.Errorf("abandon: %w", ErrAddressNotFound)
	}
	as.abandon()

	return nil
}

// SetBroadcastBeforeBlockNum sets the broadcast_before_block_num for a given chain ID
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SetBroadcastBeforeBlockNum(ctx context.Context, blockNum int64, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("set_broadcast_before_block_num: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.SetBroadcastBeforeBlockNum(ctx, blockNum, chainID); err != nil {
		return fmt.Errorf("set_broadcast_before_block_num: %w", err)
	}

	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return
		}

		for i := 0; i < len(tx.TxAttempts); i++ {
			attempt := tx.TxAttempts[i]
			if attempt.State == txmgrtypes.TxAttemptBroadcast && attempt.BroadcastBeforeBlockNum == nil {
				tx.TxAttempts[i].BroadcastBeforeBlockNum = &blockNum
			}
		}
	}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		as.ApplyToTxsByState(nil, fn)
	}

	return nil
}

// FindTxAttemptsConfirmedMissingReceipt returns all transactions that are confirmed but missing a receipt
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxAttemptsConfirmedMissingReceipt(ctx context.Context, chainID CHAIN_ID) (
	[]txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	error,
) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_next_unstarted_transaction_from_address: %w", ErrInvalidChainID)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.TxAttempts != nil && len(tx.TxAttempts) > 0
	}
	states := []txmgrtypes.TxState{TxConfirmedMissingReceipt}
	attempts := []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		attempts = append(attempts, as.FetchTxAttempts(states, filter)...)
	}
	// sort by tx_id ASC, gas_price DESC, gas_tip_cap DESC
	sort.SliceStable(attempts, func(i, j int) bool {
		return attempts[i].TxID < attempts[j].TxID
	})

	// deep copy the attempts
	var eAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, attempt := range attempts {
		eAttempts = append(eAttempts, ms.deepCopyTxAttempt(attempt.Tx, attempt))
	}

	return eAttempts, nil
}

// UpdateBroadcastAts updates the broadcast_at time for a given set of attempts
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateBroadcastAts(ctx context.Context, now time.Time, txIDs []int64) error {
	// Persist to persistent storage
	if err := ms.txStore.UpdateBroadcastAts(ctx, now, txIDs); err != nil {
		return err
	}

	// Update in memory store
	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.BroadcastAt != nil && tx.BroadcastAt.Before(now) {
			tx.BroadcastAt = &now
		}
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		as.ApplyToTxsByState(nil, fn, txIDs...)
	}

	return nil
}

// UpdateTxsUnconfirmed updates the unconfirmed transactions for a given set of ids
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxsUnconfirmed(ctx context.Context, txIDs []int64) error {
	// Persist to persistent storage
	if err := ms.txStore.UpdateTxsUnconfirmed(ctx, txIDs); err != nil {
		return err
	}

	// Update in memory store
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	wg := sync.WaitGroup{}
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, txID := range txIDs {
				if err := as.MoveConfirmedMissingReceiptToUnconfirmed(txID); err != nil {
					continue
				}
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return nil
}

// FindTxAttemptsRequiringReceiptFetch returns all transactions that are missing a receipt
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxAttemptsRequiringReceiptFetch(ctx context.Context, chainID CHAIN_ID) (
	attempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	err error,
) {
	if ms.chainID.String() != chainID.String() {
		return attempts, fmt.Errorf("find_tx_attempts_requiring_receipt_fetch: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts != nil && len(tx.TxAttempts) > 0 {
			attempt := tx.TxAttempts[0]
			return attempt.State != txmgrtypes.TxAttemptInsufficientFunds
		}

		return false
	}
	states := []txmgrtypes.TxState{TxUnconfirmed, TxConfirmedMissingReceipt}
	attempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		attempts = append(attempts, as.FetchTxAttempts(states, filterFn)...)
	}
	// sort by sequence ASC, gas_price DESC, gas_tip_cap DESC
	sort.Slice(attempts, func(i, j int) bool {
		return (*attempts[i].Tx.Sequence).Int64() < (*attempts[j].Tx.Sequence).Int64()
	})

	// deep copy the attempts
	var eAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, attempt := range attempts {
		eAttempts = append(eAttempts, ms.deepCopyTxAttempt(attempt.Tx, attempt))
	}

	return eAttempts, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxesPendingCallback(ctx context.Context, blockNum int64, chainID CHAIN_ID) (
	[]txmgrtypes.ReceiptPlus[R],
	error,
) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txes_pending_callback: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}

		if tx.TxAttempts[0].Receipts == nil || len(tx.TxAttempts[0].Receipts) == 0 {
			return false
		}

		if tx.PipelineTaskRunID.Valid && tx.SignalCallback && !tx.CallbackCompleted &&
			tx.TxAttempts[0].Receipts[0].GetBlockNumber() != nil &&
			big.NewInt(blockNum-int64(tx.MinConfirmations.Uint32)).Cmp(tx.TxAttempts[0].Receipts[0].GetBlockNumber()) > 0 {
			return true
		}

		return false

	}
	states := []txmgrtypes.TxState{TxConfirmed}
	txs := []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		txs = append(txs, as.FetchTxs(states, filterFn)...)
	}

	receiptsPlus := make([]txmgrtypes.ReceiptPlus[R], len(txs))
	meta := map[string]interface{}{}
	for i, tx := range txs {
		if err := json.Unmarshal(json.RawMessage(*tx.Meta), &meta); err != nil {
			return nil, err
		}
		failOnRevert := false
		if v, ok := meta["FailOnRevert"].(bool); ok {
			failOnRevert = v
		}

		receiptsPlus[i] = txmgrtypes.ReceiptPlus[R]{
			ID:           tx.PipelineTaskRunID.UUID,
			Receipt:      (tx.TxAttempts[0].Receipts[0]).(R),
			FailOnRevert: failOnRevert,
		}
		clear(meta)
	}

	return receiptsPlus, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxCallbackCompleted(ctx context.Context, pipelineTaskRunRid uuid.UUID, chainId CHAIN_ID) error {
	if ms.chainID.String() != chainId.String() {
		return fmt.Errorf("update_tx_callback_completed: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxCallbackCompleted(ctx, pipelineTaskRunRid, chainId); err != nil {
		return err
	}

	// Update in memory store
	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.PipelineTaskRunID.UUID == pipelineTaskRunRid {
			tx.CallbackCompleted = true
		}
	}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			as.ApplyToTxsByState(nil, fn)
			wg.Done()
		}(as)
	}
	wg.Wait()

	return nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveFetchedReceipts(ctx context.Context, receipts []R, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("save_fetched_receipts: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveFetchedReceipts(ctx, receipts, chainID); err != nil {
		return err
	}

	// Update in memory store
	errsLock := sync.Mutex{}
	var errs error
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, receipt := range receipts {
				if err := as.MoveUnconfirmedToConfirmed(receipt); err != nil {
					errsLock.Lock()
					errs = errors.Join(errs, err)
					errsLock.Unlock()
				}
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return errs
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxesByMetaFieldAndStates(ctx context.Context, metaField string, metaValue string, states []txmgrtypes.TxState, chainID *big.Int) (
	[]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	error,
) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txes_by_meta_field_and_states: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.Meta == nil {
			return false
		}
		meta := map[string]interface{}{}
		if err := json.Unmarshal(json.RawMessage(*tx.Meta), &meta); err != nil {
			return false
		}
		if v, ok := meta[metaField].(string); ok {
			return v == metaValue
		}

		return false
	}
	txsLock := sync.Mutex{}
	txs := []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, tx := range as.FetchTxs(states, filterFn) {
				etx := ms.deepCopyTx(tx)
				txsLock.Lock()
				txs = append(txs, etx)
				txsLock.Unlock()
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return txs, nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxesWithMetaFieldByStates(ctx context.Context, metaField string, states []txmgrtypes.TxState, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txes_with_meta_field_by_states: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.Meta == nil {
			return false
		}
		meta := map[string]interface{}{}
		if err := json.Unmarshal(json.RawMessage(*tx.Meta), &meta); err != nil {
			return false
		}
		if _, ok := meta[metaField]; ok {
			return true
		}

		return false
	}

	txsLock := sync.Mutex{}
	txs := []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, tx := range as.FetchTxs(states, filterFn) {
				etx := ms.deepCopyTx(tx)
				txsLock.Lock()
				txs = append(txs, etx)
				txsLock.Unlock()
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return txs, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxesWithMetaFieldByReceiptBlockNum(ctx context.Context, metaField string, blockNum int64, chainID *big.Int) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txes_with_meta_field_by_receipt_block_num: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.Meta == nil {
			return false
		}
		meta := map[string]interface{}{}
		if err := json.Unmarshal(json.RawMessage(*tx.Meta), &meta); err != nil {
			return false
		}
		if _, ok := meta[metaField]; !ok {
			return false
		}
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if attempt.Receipts == nil || len(attempt.Receipts) == 0 {
			return false
		}
		if attempt.Receipts[0].GetBlockNumber() == nil {
			return false
		}

		return attempt.Receipts[0].GetBlockNumber().Int64() >= blockNum
	}

	txsLock := sync.Mutex{}
	txs := []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, tx := range as.FetchTxs(nil, filterFn) {
				etx := ms.deepCopyTx(tx)
				txsLock.Lock()
				txs = append(txs, etx)
				txsLock.Unlock()
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return txs, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxesWithAttemptsAndReceiptsByIdsAndState(ctx context.Context, ids []big.Int, states []txmgrtypes.TxState, chainID *big.Int) (tx []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], err error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txes_with_attempts_and_receipts_by_ids_and_state: %w", ErrInvalidChainID)
	}

	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return true
	}

	txIDs := make([]int64, len(ids))
	for i, id := range ids {
		txIDs[i] = id.Int64()
	}

	txsLock := sync.Mutex{}
	txs := []*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			for _, tx := range as.FetchTxs(states, filterFn, txIDs...) {
				etx := ms.deepCopyTx(tx)
				txsLock.Lock()
				txs = append(txs, etx)
				txsLock.Unlock()
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return txs, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) PruneUnstartedTxQueue(ctx context.Context, queueSize uint32, subject uuid.UUID) ([]int64, error) {
	// Persist to persistent storage
	ids, err := ms.txStore.PruneUnstartedTxQueue(ctx, queueSize, subject)
	if err != nil {
		return ids, err
	}

	// Update in memory store
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		as.PruneUnstartedTxQueue(ids)
	}

	return ids, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) ReapTxHistory(ctx context.Context, minBlockNumberToKeep int64, timeThreshold time.Time, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("reap_tx_history: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.ReapTxHistory(ctx, minBlockNumberToKeep, timeThreshold, chainID); err != nil {
		return err
	}

	// Update in memory store
	states := []txmgrtypes.TxState{TxConfirmed}
	filterFn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if attempt.Receipts == nil || len(attempt.Receipts) == 0 {
			return false
		}
		if attempt.Receipts[0].GetBlockNumber() == nil {
			return false
		}
		if attempt.Receipts[0].GetBlockNumber().Int64() >= minBlockNumberToKeep {
			return false
		}
		if tx.CreatedAt.After(timeThreshold) {
			return false
		}
		if tx.State != TxConfirmed {
			return false
		}
		return true
	}

	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			as.DeleteTxs(as.FetchTxs(states, filterFn)...)
			wg.Done()
		}(as)
	}
	wg.Wait()

	filterFn = func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.State == TxFatalError && tx.CreatedAt.Before(timeThreshold)
	}
	states = []txmgrtypes.TxState{TxFatalError}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			as.DeleteTxs(as.FetchTxs(states, filterFn)...)
			wg.Done()
		}(as)
	}
	wg.Wait()

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountTransactionsByState(_ context.Context, state txmgrtypes.TxState, chainID CHAIN_ID) (uint32, error) {
	if ms.chainID.String() != chainID.String() {
		return 0, fmt.Errorf("count_transactions_by_state: %w", ErrInvalidChainID)
	}

	var total int
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		total += as.CountTransactionsByState(state)
	}

	return uint32(total), nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) DeleteInProgressAttempt(ctx context.Context, attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("delete_in_progress_attempt: expected attempt to be in_progress")
	}
	if attempt.ID == 0 {
		return fmt.Errorf("delete_in_progress_attempt: expected attempt to have an ID")
	}

	// Check if fromaddress enabled
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("delete_in_progress_attempt: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.DeleteInProgressAttempt(ctx, attempt); err != nil {
		return fmt.Errorf("delete_in_progress_attempt: %w", err)
	}

	// Update in memory store
	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}

		return tx.TxAttempts[0].ID == attempt.ID
	}
	as.DeleteTxs(as.FetchTxs(nil, filter)...)

	return nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxsRequiringResubmissionDueToInsufficientFunds(_ context.Context, address ADDR, chainID CHAIN_ID) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txs_requiring_resubmission_due_to_insufficient_funds: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[address]
	if !ok {
		return nil, fmt.Errorf("find_txs_requiring_resubmission_due_to_insufficient_funds: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]

		return attempt.State == txmgrtypes.TxAttemptInsufficientFunds
	}
	states := []txmgrtypes.TxState{TxUnconfirmed}
	txs := as.FetchTxs(states, filter)
	// sort by sequence ASC
	sort.Slice(txs, func(i, j int) bool {
		return (*txs[i].Sequence).Int64() < (*txs[j].Sequence).Int64()
	})

	etxs := make([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs))
	for i, tx := range txs {
		etxs[i] = ms.deepCopyTx(tx)
	}

	return etxs, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxAttemptsRequiringResend(_ context.Context, olderThan time.Time, maxInFlightTransactions uint32, chainID CHAIN_ID, address ADDR) ([]txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_tx_attempts_requiring_resend: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[address]
	if !ok {
		return nil, fmt.Errorf("find_tx_attempts_requiring_resend: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if attempt.State == txmgrtypes.TxAttemptInProgress {
			return false
		}
		if tx.BroadcastAt.After(olderThan) {
			return false
		}

		return false
	}
	states := []txmgrtypes.TxState{TxUnconfirmed, TxConfirmedMissingReceipt}
	attempts := as.FetchTxAttempts(states, filter)
	// sort by sequence ASC, gas_price DESC, gas_tip_cap DESC
	sort.Slice(attempts, func(i, j int) bool {
		return (*attempts[i].Tx.Sequence).Int64() < (*attempts[j].Tx.Sequence).Int64()
	})
	// LIMIT by maxInFlightTransactions
	if len(attempts) > int(maxInFlightTransactions) {
		attempts = attempts[:maxInFlightTransactions]
	}

	// deep copy the attempts
	var eAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, attempt := range attempts {
		eAttempts = append(eAttempts, ms.deepCopyTxAttempt(attempt.Tx, attempt))
	}

	return eAttempts, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxWithSequence(_ context.Context, fromAddress ADDR, seq SEQ) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[fromAddress]
	if !ok {
		return nil, fmt.Errorf("find_tx_with_sequence: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.Sequence == nil {
			return false
		}

		return (*tx.Sequence).String() == seq.String()
	}
	states := []txmgrtypes.TxState{TxConfirmed, TxConfirmedMissingReceipt, TxUnconfirmed}
	txs := as.FetchTxs(states, filter)
	if len(txs) == 0 {
		return nil, nil
	}

	return ms.deepCopyTx(txs[0]), nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTransactionsConfirmedInBlockRange(_ context.Context, highBlockNumber, lowBlockNumber int64, chainID CHAIN_ID) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_transactions_confirmed_in_block_range: %w", ErrInvalidChainID)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if attempt.State != txmgrtypes.TxAttemptBroadcast {
			return false
		}
		if len(attempt.Receipts) == 0 {
			return false
		}
		if attempt.Receipts[0].GetBlockNumber() == nil {
			return false
		}
		blockNum := attempt.Receipts[0].GetBlockNumber().Int64()
		return blockNum >= lowBlockNumber && blockNum <= highBlockNumber
	}
	states := []txmgrtypes.TxState{TxConfirmed, TxConfirmedMissingReceipt}
	txsLock := sync.Mutex{}
	txs := []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			ts := as.FetchTxs(states, filter)
			txsLock.Lock()
			txs = append(txs, ts...)
			txsLock.Unlock()
			wg.Done()
		}(as)
	}
	wg.Wait()
	// sort by sequence ASC
	sort.Slice(txs, func(i, j int) bool {
		return (*txs[i].Sequence).Int64() < (*txs[j].Sequence).Int64()
	})

	etxs := make([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs))
	for i, tx := range txs {
		etxs[i] = ms.deepCopyTx(tx)
	}

	return etxs, nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindEarliestUnconfirmedBroadcastTime(ctx context.Context, chainID CHAIN_ID) (null.Time, error) {
	if ms.chainID.String() != chainID.String() {
		return null.Time{}, fmt.Errorf("find_earliest_unconfirmed_broadcast_time: %w", ErrInvalidChainID)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.InitialBroadcastAt != nil
	}
	states := []txmgrtypes.TxState{TxUnconfirmed}
	txsLock := sync.Mutex{}
	txs := []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			etxs := as.FetchTxs(states, filter)
			txsLock.Lock()
			txs = append(txs, etxs...)
			txsLock.Unlock()
			wg.Done()
		}(as)
	}
	wg.Wait()

	var minInitialBroadcastAt time.Time
	for _, tx := range txs {
		if tx.InitialBroadcastAt.Before(minInitialBroadcastAt) {
			minInitialBroadcastAt = *tx.InitialBroadcastAt
		}
	}

	return null.TimeFrom(minInitialBroadcastAt), nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindEarliestUnconfirmedTxAttemptBlock(ctx context.Context, chainID CHAIN_ID) (null.Int, error) {
	if ms.chainID.String() != chainID.String() {
		return null.Int{}, fmt.Errorf("find_earliest_unconfirmed_broadcast_time: %w", ErrInvalidChainID)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		return attempt.BroadcastBeforeBlockNum != nil
	}
	states := []txmgrtypes.TxState{TxUnconfirmed}
	txsLock := sync.Mutex{}
	txs := []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			etxs := as.FetchTxs(states, filter)
			txsLock.Lock()
			txs = append(txs, etxs...)
			txsLock.Unlock()
			wg.Done()
		}(as)
	}
	wg.Wait()

	var minBroadcastBeforeBlockNum int64
	for _, tx := range txs {
		if *tx.TxAttempts[0].BroadcastBeforeBlockNum < minBroadcastBeforeBlockNum {
			minBroadcastBeforeBlockNum = *tx.TxAttempts[0].BroadcastBeforeBlockNum
		}
	}

	return null.IntFrom(minBroadcastBeforeBlockNum), nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) GetInProgressTxAttempts(ctx context.Context, address ADDR, chainID CHAIN_ID) ([]txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("get_in_progress_tx_attempts: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[address]
	if !ok {
		return nil, fmt.Errorf("get_in_progress_tx_attempts: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		return attempt.State == txmgrtypes.TxAttemptInProgress
	}
	states := []txmgrtypes.TxState{TxConfirmed, TxConfirmedMissingReceipt, TxUnconfirmed}
	attempts := as.FetchTxAttempts(states, filter)

	// deep copy the attempts
	var eAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, attempt := range attempts {
		eAttempts = append(eAttempts, ms.deepCopyTxAttempt(attempt.Tx, attempt))
	}

	return eAttempts, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) GetNonFatalTransactions(ctx context.Context, chainID CHAIN_ID) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("get_non_fatal_transactions: %w", ErrInvalidChainID)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.State != TxFatalError
	}
	txsLock := sync.Mutex{}
	txs := []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	wg := sync.WaitGroup{}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			etxs := as.FetchTxs(nil, filter)
			txsLock.Lock()
			txs = append(txs, etxs...)
			txsLock.Unlock()
			wg.Done()
		}(as)
	}
	wg.Wait()

	etxs := make([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs))
	for i, tx := range txs {
		etxs[i] = ms.deepCopyTx(tx)
	}

	return etxs, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) GetTxByID(_ context.Context, id int64) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.ID == id
	}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		txs := as.FetchTxs(nil, filter, id)
		if len(txs) > 0 {
			return ms.deepCopyTx(txs[0]), nil
		}
	}

	return nil, fmt.Errorf("failed to get tx with id: %v", id)

}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) HasInProgressTransaction(_ context.Context, account ADDR, chainID CHAIN_ID) (bool, error) {
	if ms.chainID.String() != chainID.String() {
		return false, fmt.Errorf("has_in_progress_transaction: %w", ErrInvalidChainID)
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[account]
	if !ok {
		return false, fmt.Errorf("has_in_progress_transaction: %w", ErrAddressNotFound)
	}

	n := as.CountTransactionsByState(TxInProgress)

	return n > 0, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) LoadTxAttempts(_ context.Context, etx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[etx.FromAddress]
	if !ok {
		return fmt.Errorf("load_tx_attempts: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return tx.ID == etx.ID
	}
	txAttempts := []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	for _, tx := range as.FetchTxs(nil, filter, etx.ID) {
		for _, txAttempt := range tx.TxAttempts {
			txAttempts = append(txAttempts, ms.deepCopyTxAttempt(*etx, txAttempt))
		}
	}
	etx.TxAttempts = txAttempts

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) PreloadTxes(_ context.Context, attempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	if len(attempts) == 0 {
		return nil
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempts[0].Tx.FromAddress]
	if !ok {
		return fmt.Errorf("preload_txes: %w", ErrAddressNotFound)
	}

	txIDs := make([]int64, len(attempts))
	for i, attempt := range attempts {
		txIDs[i] = attempt.TxID
	}
	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		return true
	}
	txs := as.FetchTxs(nil, filter, txIDs...)
	for i, attempt := range attempts {
		for _, tx := range txs {
			if tx.ID == attempt.TxID {
				attempts[i].Tx = *ms.deepCopyTx(tx)
			}
		}
	}

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveConfirmedMissingReceiptAttempt(ctx context.Context, timeout time.Duration, attempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], broadcastAt time.Time) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("save_confirmed_missing_receipt_attempt: %w", ErrAddressNotFound)
	}
	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("expected state to be in_progress")
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveConfirmedMissingReceiptAttempt(ctx, timeout, attempt, broadcastAt); err != nil {
		return err
	}

	// Update in memory store
	return as.MoveInProgressToConfirmedMissingReceipt(*attempt, broadcastAt)
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveInProgressAttempt(ctx context.Context, attempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("save_in_progress_attempt: %w", ErrAddressNotFound)
	}
	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("SaveInProgressAttempt failed: attempt state must be in_progress")
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveInProgressAttempt(ctx, attempt); err != nil {
		return err
	}

	// Update in memory store
	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.ID != attempt.TxID {
			return
		}
		if tx.TxAttempts != nil && len(tx.TxAttempts) > 0 {
			for i := 0; i < len(tx.TxAttempts); i++ {
				if tx.TxAttempts[i].ID == attempt.ID {
					tx.TxAttempts[i].State = txmgrtypes.TxAttemptInProgress
					tx.TxAttempts[i].BroadcastBeforeBlockNum = attempt.BroadcastBeforeBlockNum
					return
				}
			}
		}
		tx.TxAttempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{*attempt}
	}
	as.ApplyToTxsByState(nil, fn, attempt.TxID)

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveInsufficientFundsAttempt(ctx context.Context, timeout time.Duration, attempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], broadcastAt time.Time) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("save_insufficient_funds_attempt: %w", ErrAddressNotFound)
	}
	if !(attempt.State == txmgrtypes.TxAttemptInProgress || attempt.State == txmgrtypes.TxAttemptInsufficientFunds) {
		return fmt.Errorf("expected state to be in_progress or insufficient_funds")
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveInsufficientFundsAttempt(ctx, timeout, attempt, broadcastAt); err != nil {
		return err
	}

	// Update in memory store
	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.ID != attempt.TxID {
			return
		}
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return
		}
		if tx.BroadcastAt.Before(broadcastAt) {
			tx.BroadcastAt = &broadcastAt
		}

		tx.TxAttempts[0].State = txmgrtypes.TxAttemptInsufficientFunds
	}
	as.ApplyToTxsByState(nil, fn, attempt.TxID)

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) SaveSentAttempt(ctx context.Context, timeout time.Duration, attempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], broadcastAt time.Time) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[attempt.Tx.FromAddress]
	if !ok {
		return fmt.Errorf("save_sent_attempt: %w", ErrAddressNotFound)
	}

	if attempt.State != txmgrtypes.TxAttemptInProgress {
		return fmt.Errorf("expected state to be in_progress")
	}

	// Persist to persistent storage
	if err := ms.txStore.SaveSentAttempt(ctx, timeout, attempt, broadcastAt); err != nil {
		return err
	}

	// Update in memory store
	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
		if tx.ID != attempt.TxID {
			return
		}
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return
		}
		if tx.BroadcastAt.Before(broadcastAt) {
			tx.BroadcastAt = &broadcastAt
		}

		tx.TxAttempts[0].State = txmgrtypes.TxAttemptBroadcast
	}
	as.ApplyToTxsByState(nil, fn, attempt.TxID)

	return nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxForRebroadcast(ctx context.Context, etx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], etxAttempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[etx.FromAddress]
	if !ok {
		return fmt.Errorf("update_tx_for_rebroadcast: %w", ErrAddressNotFound)
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxForRebroadcast(ctx, etx, etxAttempt); err != nil {
		return err
	}

	// Update in memory store
	return as.MoveConfirmedToUnconfirmed(etxAttempt)
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) IsTxFinalized(ctx context.Context, blockHeight int64, txID int64, chainID CHAIN_ID) (bool, error) {
	if ms.chainID.String() != chainID.String() {
		return false, fmt.Errorf("is_tx_finalized: %w", ErrInvalidChainID)
	}

	fn := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.ID != txID {
			return false
		}
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if attempt.Receipts == nil || len(attempt.Receipts) == 0 {
			return false
		}
		if attempt.Receipts[0].GetBlockNumber() == nil {
			return false
		}

		return attempt.Receipts[0].GetBlockNumber().Int64() <= (blockHeight - int64(tx.MinConfirmations.Uint32))
	}
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		txas := as.FetchTxAttempts(nil, fn, txID)
		if len(txas) > 0 {
			return true, nil
		}
	}

	return false, nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxsRequiringGasBump(ctx context.Context, address ADDR, blockNum, gasBumpThreshold, depth int64, chainID CHAIN_ID) ([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_txs_requiring_gas_bump: %w", ErrInvalidChainID)
	}
	if gasBumpThreshold == 0 {
		return nil, nil
	}

	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	as, ok := ms.addressStates[address]
	if !ok {
		return nil, fmt.Errorf("find_txs_requiring_gas_bump: %w", ErrAddressNotFound)
	}

	filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
		if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
			return false
		}
		attempt := tx.TxAttempts[0]
		if *attempt.BroadcastBeforeBlockNum <= blockNum ||
			attempt.State == txmgrtypes.TxAttemptBroadcast {
			return false
		}

		if tx.State != TxUnconfirmed ||
			attempt.ID != 0 {
			return false
		}

		return true
	}
	states := []txmgrtypes.TxState{TxUnconfirmed}
	txs := as.FetchTxs(states, filter)
	// sort by sequence ASC
	sort.Slice(txs, func(i, j int) bool {
		return (*txs[i].Sequence).Int64() < (*txs[j].Sequence).Int64()
	})

	if depth > 0 {
		// LIMIT by depth
		if len(txs) > int(depth) {
			txs = txs[:depth]
		}
	}

	etxs := make([]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs))
	for i, tx := range txs {
		etxs[i] = ms.deepCopyTx(tx)
	}

	return etxs, nil
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MarkAllConfirmedMissingReceipt(ctx context.Context, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("mark_all_confirmed_missing_receipt: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.MarkAllConfirmedMissingReceipt(ctx, chainID); err != nil {
		return err
	}

	// Update in memory store
	wg := sync.WaitGroup{}
	errsLock := sync.Mutex{}
	var errs error
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			// TODO(jtw): THIS IS EVM SPECIFIC THIS SHOULD BE GENERALIZED
			// Get the max confirmed sequence
			filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool { return true }
			states := []txmgrtypes.TxState{TxConfirmed}
			txs := as.FetchTxs(states, filter)
			var maxConfirmedSequence SEQ
			for _, tx := range txs {
				if tx.Sequence == nil {
					continue
				}
				if (*tx.Sequence).Int64() > maxConfirmedSequence.Int64() {
					maxConfirmedSequence = *tx.Sequence
				}
			}

			// Mark all unconfirmed txs with a sequence less than the max confirmed sequence as confirmed_missing_receipt
			filter = func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
				if tx.Sequence == nil {
					return false
				}
				if tx.State != TxUnconfirmed {
					return false
				}

				return (*tx.Sequence).Int64() < maxConfirmedSequence.Int64()
			}
			states = []txmgrtypes.TxState{TxUnconfirmed}
			txs = as.FetchTxs(states, filter)
			for _, tx := range txs {
				attempt := tx.TxAttempts[0]

				if err := as.MoveUnconfirmedToConfirmedMissingReceipt(attempt, *tx.BroadcastAt); err != nil {
					err = fmt.Errorf("mark_all_confirmed_missing_receipt: address: %s: %w", as.fromAddress, err)
					errsLock.Lock()
					errs = errors.Join(errs, err)
					errsLock.Unlock()
				}
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	return errs
}
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MarkOldTxesMissingReceiptAsErrored(ctx context.Context, blockNum int64, finalityDepth uint32, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("mark_old_txes_missing_receipt_as_errored: %w", ErrInvalidChainID)
	}

	// Persist to persistent storage
	if err := ms.txStore.MarkOldTxesMissingReceiptAsErrored(ctx, blockNum, finalityDepth, chainID); err != nil {
		return err
	}

	// Update in memory store
	type result struct {
		ID                         int64
		Sequence                   SEQ
		FromAddress                ADDR
		MaxBroadcastBeforeBlockNum int64
		TxHashes                   []TX_HASH
	}
	var resultsLock sync.Mutex
	var results []result
	wg := sync.WaitGroup{}
	errsLock := sync.Mutex{}
	var errs error
	ms.addressStatesLock.RLock()
	defer ms.addressStatesLock.RUnlock()
	for _, as := range ms.addressStates {
		wg.Add(1)
		go func(as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) {
			filter := func(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool {
				if tx.TxAttempts == nil || len(tx.TxAttempts) == 0 {
					return false
				}
				if tx.State != TxConfirmedMissingReceipt {
					return false
				}
				attempt := tx.TxAttempts[0]
				if attempt.BroadcastBeforeBlockNum == nil {
					return false
				}

				return *attempt.BroadcastBeforeBlockNum < blockNum-int64(finalityDepth)
			}
			states := []txmgrtypes.TxState{TxConfirmedMissingReceipt}
			txs := as.FetchTxs(states, filter)
			for _, tx := range txs {
				if err := as.MoveConfirmedMissingReceiptToFatalError(tx, null.StringFrom(ErrCouldNotGetReceipt.Error())); err != nil {
					err = fmt.Errorf("mark_old_txes_missing_receipt_as_errored: address: %s: %w", as.fromAddress, err)
					errsLock.Lock()
					errs = errors.Join(errs, err)
					errsLock.Unlock()
					continue
				}
				hashes := make([]TX_HASH, len(tx.TxAttempts))
				maxBroadcastBeforeBlockNum := int64(0)
				for i, attempt := range tx.TxAttempts {
					hashes[i] = attempt.Hash
					if attempt.BroadcastBeforeBlockNum != nil {
						if *attempt.BroadcastBeforeBlockNum > maxBroadcastBeforeBlockNum {
							maxBroadcastBeforeBlockNum = *attempt.BroadcastBeforeBlockNum
						}
					}
				}
				rr := result{
					ID:                         tx.ID,
					Sequence:                   *tx.Sequence,
					FromAddress:                tx.FromAddress,
					MaxBroadcastBeforeBlockNum: maxBroadcastBeforeBlockNum,
					TxHashes:                   hashes,
				}
				resultsLock.Lock()
				results = append(results, rr)
				resultsLock.Unlock()
			}
			wg.Done()
		}(as)
	}
	wg.Wait()

	for _, r := range results {
		ms.lggr.Criticalw(fmt.Sprintf("eth_tx with ID %v expired without ever getting a receipt for any of our attempts. "+
			"Current block height is %v, transaction was broadcast before block height %v. This transaction may not have not been sent and will be marked as fatally errored. "+
			"This can happen if there is another instance of chainlink running that is using the same private key, or if "+
			"an external wallet has been used to send a transaction from account %s with nonce %v."+
			" Please note that Chainlink requires exclusive ownership of it's private keys and sharing keys across multiple"+
			" chainlink instances, or using the chainlink keys with an external wallet is NOT SUPPORTED and WILL lead to missed transactions",
			r.ID, blockNum, r.MaxBroadcastBeforeBlockNum, r.FromAddress, r.Sequence), "ethTxID", r.ID, "sequence", r.Sequence, "fromAddress", r.FromAddress, "txHashes", r.TxHashes)
	}

	return errs
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) deepCopyTx(
	tx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	copyTx := txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{
		ID:                 tx.ID,
		IdempotencyKey:     tx.IdempotencyKey,
		Sequence:           tx.Sequence,
		FromAddress:        tx.FromAddress,
		ToAddress:          tx.ToAddress,
		EncodedPayload:     make([]byte, len(tx.EncodedPayload)),
		Value:              *new(big.Int).Set(&tx.Value),
		FeeLimit:           tx.FeeLimit,
		Error:              tx.Error,
		BroadcastAt:        tx.BroadcastAt,
		InitialBroadcastAt: tx.InitialBroadcastAt,
		CreatedAt:          tx.CreatedAt,
		State:              tx.State,
		TxAttempts:         make([]txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(tx.TxAttempts)),
		Meta:               tx.Meta,
		Subject:            tx.Subject,
		ChainID:            tx.ChainID,
		PipelineTaskRunID:  tx.PipelineTaskRunID,
		MinConfirmations:   tx.MinConfirmations,
		TransmitChecker:    tx.TransmitChecker,
		SignalCallback:     tx.SignalCallback,
		CallbackCompleted:  tx.CallbackCompleted,
	}

	// Copy the EncodedPayload
	copy(copyTx.EncodedPayload, tx.EncodedPayload)

	// Copy the TxAttempts
	for i, attempt := range tx.TxAttempts {
		copyTx.TxAttempts[i] = ms.deepCopyTxAttempt(copyTx, attempt)
	}

	return &copyTx
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) deepCopyTxAttempt(
	tx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	copyAttempt := txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{
		ID:                      attempt.ID,
		TxID:                    attempt.TxID,
		Tx:                      tx,
		TxFee:                   attempt.TxFee,
		ChainSpecificFeeLimit:   attempt.ChainSpecificFeeLimit,
		SignedRawTx:             make([]byte, len(attempt.SignedRawTx)),
		Hash:                    attempt.Hash,
		CreatedAt:               attempt.CreatedAt,
		BroadcastBeforeBlockNum: attempt.BroadcastBeforeBlockNum,
		State:                   attempt.State,
		Receipts:                make([]txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH], len(attempt.Receipts)),
		TxType:                  attempt.TxType,
	}

	copy(copyAttempt.SignedRawTx, attempt.SignedRawTx)
	copy(copyAttempt.Receipts, attempt.Receipts)

	return copyAttempt
}