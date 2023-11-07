package txmgr

import (
	"context"
	"fmt"
	"sync"

	feetypes "github.com/smartcontractkit/chainlink/v2/common/fee/types"
	txmgrtypes "github.com/smartcontractkit/chainlink/v2/common/txmgr/types"
	"github.com/smartcontractkit/chainlink/v2/common/types"
	"github.com/smartcontractkit/chainlink/v2/core/chains/evm/label"
	"gopkg.in/guregu/null.v4"
)

var (
	// ErrInvalidChainID is returned when the chain ID is invalid
	ErrInvalidChainID = fmt.Errorf("invalid chain ID")
	// ErrTxnNotFound is returned when a transaction is not found
	ErrTxnNotFound = fmt.Errorf("transaction not found")
	// ErrExistingIdempotencyKey is returned when a transaction with the same idempotency key already exists
	ErrExistingIdempotencyKey = fmt.Errorf("transaction with idempotency key already exists")
	// ErrAddressNotFound is returned when an address is not found
	ErrAddressNotFound = fmt.Errorf("address not found")
)

// Store and update all transaction state as files
// Read from the files to restore state at startup
// Delete files when transactions are completed or reaped

// Life of a Transaction
// 1. Transaction Request is created
// 2. Transaction Request is submitted to the Transaction Manager
// 3. Transaction Manager creates and persists a new transaction (unstarted) from the transaction request (not persisted)
// 4. Transaction Manager sends the transaction (unstarted) to the Broadcaster Unstarted Queue
// 4. Transaction Manager prunes the Unstarted Queue based on the transaction prune strategy

// NOTE(jtw): Only one transaction per address can be in_progress at a time
// NOTE(jtw): Only one broadcasted attempt exists per transaction the rest are errored or abandoned
// 1. Broadcaster assigns a sequence number to the transaction
// 2. Broadcaster creates and persists a new transaction attempt (in_progress) from the transaction (in_progress)
// 3. Broadcaster asks the Checker to check if the transaction should not be sent
// 4. Broadcaster asks the Attempt builder to figure out gas fee for the transaction
// 5. Broadcaster attempts to send the Transaction to TransactionClient to be published on-chain
// 6. Broadcaster updates the transaction attempt (broadcast) and transaction (unconfirmed)
// 7. Broadcaster increments global sequence number for address for next transaction attempt

// NOTE(jtw): Only one receipt should exist per confirmed transaction
// 1. Confirmer listens and reads new Head events from the Chain
// 2. Confirmer sets the last known block number for the transaction attempts that have been broadcast
// 3. Confirmer checks for missing receipts for transactions that have been broadcast
// 4. Confirmer sets transactions that have failed to (unconfirmed) which will be retried by the resender
// 5. Confirmer sets transactions that have been confirmed to (confirmed) and creates a new receipt which is persisted

type InMemoryStore[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
] struct {
	chainID CHAIN_ID

	keyStore txmgrtypes.KeyStore[ADDR, CHAIN_ID, SEQ]
	txStore  txmgrtypes.TxStore[ADDR, CHAIN_ID, TX_HASH, BLOCK_HASH, R, SEQ, FEE]

	pendingLock sync.RWMutex
	// NOTE(jtw): we might need to watch out for txns that finish and are removed from the pending map
	pendingIdempotencyKeys map[string]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]

	// unstarted is a map of addresses to a channel of unstarted transactions
	unstarted map[ADDR]chan *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	// inprogress is a map of addresses to inprogress transactions
	inprogressLock sync.RWMutex
	inprogress     map[ADDR]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	// unconfirmed is a map of addresses to a map of transaction IDs to unconfirmed transactions
	unconfirmedLock sync.RWMutex
	unconfirmed     map[ADDR]map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
}

// NewInMemoryStore returns a new InMemoryStore
func NewInMemoryStore[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
](
	chainID CHAIN_ID,
	keyStore txmgrtypes.KeyStore[ADDR, CHAIN_ID, SEQ],
	txStore txmgrtypes.TxStore[ADDR, CHAIN_ID, TX_HASH, BLOCK_HASH, R, SEQ, FEE],
) (*InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE], error) {
	ms := InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]{
		chainID:  chainID,
		keyStore: keyStore,
		txStore:  txStore,

		pendingIdempotencyKeys: map[string]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{},

		unstarted:   map[ADDR]chan *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{},
		inprogress:  map[ADDR]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{},
		unconfirmed: map[ADDR]map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{},
	}

	addresses, err := keyStore.EnabledAddressesForChain(chainID)
	if err != nil {
		return nil, fmt.Errorf("new_in_memory_store: %w", err)
	}
	for _, fromAddr := range addresses {
		// Channel Buffer is set to something high to prevent blocking and allow the pruning to happen
		ms.unstarted[fromAddr] = make(chan *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], 100)
		ms.unconfirmed[fromAddr] = map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	}

	return &ms, nil
}

// CreateTransaction creates a new transaction for a given txRequest.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CreateTransaction(ctx context.Context, txRequest txmgrtypes.TxRequest[ADDR, TX_HASH], chainID CHAIN_ID) (tx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], err error) {
	if ms.chainID.String() != chainID.String() {
		return tx, fmt.Errorf("create_transaction: %w", ErrInvalidChainID)
	}
	if _, ok := ms.unstarted[txRequest.FromAddress]; !ok {
		return tx, fmt.Errorf("create_transaction: %w", ErrAddressNotFound)
	}

	// Persist Transaction to persistent storage
	tx, err = ms.txStore.CreateTransaction(ctx, txRequest, chainID)
	if err != nil {
		return tx, fmt.Errorf("create_transaction: %w", err)
	}
	if err := ms.sendTxToUnstartedQueue(tx); err != nil {
		return tx, fmt.Errorf("create_transaction: %w", err)
	}

	return tx, nil
}

// FindTxWithIdempotencyKey returns a transaction with the given idempotency key
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxWithIdempotencyKey(ctx context.Context, idempotencyKey string, chainID CHAIN_ID) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	if ms.chainID.String() != chainID.String() {
		return nil, fmt.Errorf("find_tx_with_idempotency_key: %w", ErrInvalidChainID)
	}

	ms.pendingLock.Lock()
	defer ms.pendingLock.Unlock()

	tx, ok := ms.pendingIdempotencyKeys[idempotencyKey]
	if !ok {
		return nil, fmt.Errorf("find_tx_with_idempotency_key: %w", ErrTxnNotFound)
	}

	return tx, nil
}

// CheckTxQueueCapacity checks if the queue capacity has been reached for a given address
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CheckTxQueueCapacity(ctx context.Context, fromAddress ADDR, maxQueuedTransactions uint64, chainID CHAIN_ID) error {
	if maxQueuedTransactions == 0 {
		return nil
	}
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("check_tx_queue_capacity: %w", ErrInvalidChainID)
	}
	if _, ok := ms.unstarted[fromAddress]; !ok {
		return fmt.Errorf("check_tx_queue_capacity: %w", ErrAddressNotFound)
	}

	count := uint64(len(ms.unstarted[fromAddress]))
	if count >= maxQueuedTransactions {
		return fmt.Errorf("check_tx_queue_capacity: cannot create transaction; too many unstarted transactions in the queue (%v/%v). %s", count, maxQueuedTransactions, label.MaxQueuedTransactionsWarning)
	}

	return nil
}

/////
// BROADCASTER FUNCTIONS
/////

// FindLatestSequence returns the latest sequence number for a given address
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindLatestSequence(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (seq SEQ, err error) {
	// query the persistent storage since this method only gets called when the broadcaster is starting up.
	// It is used to initialize the in-memory sequence map in the broadcaster
	// NOTE(jtw): should the nextSequenceMap be moved to the in-memory store?

	if ms.chainID.String() != chainID.String() {
		return seq, fmt.Errorf("find_latest_sequence: %w", ErrInvalidChainID)
	}
	if _, ok := ms.unstarted[fromAddress]; !ok {
		return seq, fmt.Errorf("find_latest_sequence: %w", ErrAddressNotFound)
	}

	seq, err = ms.txStore.FindLatestSequence(ctx, fromAddress, chainID)
	if err != nil {
		return seq, fmt.Errorf("find_latest_sequence: %w", err)
	}

	return seq, nil
}

// CountUnconfirmedTransactions returns the number of unconfirmed transactions for a given address.
// Unconfirmed transactions are transactions that have been broadcast but not confirmed on-chain.
// NOTE(jtw): used to calculate total inflight transactions
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountUnconfirmedTransactions(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (uint32, error) {
	if ms.chainID.String() != chainID.String() {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrInvalidChainID)
	}
	u, ok := ms.unconfirmed[fromAddress]
	if !ok {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrAddressNotFound)
	}

	return uint32(len(u)), nil
}

// CountUnstartedTransactions returns the number of unstarted transactions for a given address.
// Unstarted transactions are transactions that have not been broadcast yet.
// NOTE(jtw): used to calculate total inflight transactions
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountUnstartedTransactions(ctx context.Context, fromAddress ADDR, chainID CHAIN_ID) (uint32, error) {
	if ms.chainID.String() != chainID.String() {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrInvalidChainID)
	}
	if _, ok := ms.unstarted[fromAddress]; !ok {
		return 0, fmt.Errorf("count_unstarted_transactions: %w", ErrAddressNotFound)
	}

	return uint32(len(ms.unstarted[fromAddress])), nil
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

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxUnstartedToInProgress(ctx, tx, attempt); err != nil {
		return fmt.Errorf("update_tx_unstarted_to_in_progress: %w", err)
	}
	tx.TxAttempts = append(tx.TxAttempts, *attempt)

	// Update in memory store
	ms.inprogressLock.Lock()
	ms.inprogress[tx.FromAddress] = tx
	ms.inprogressLock.Unlock()

	return nil
}

// GetTxInProgress returns the in_progress transaction for a given address.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) GetTxInProgress(ctx context.Context, fromAddress ADDR) (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	ms.inprogressLock.RLock()
	defer ms.inprogressLock.RUnlock()
	tx, ok := ms.inprogress[fromAddress]
	if !ok {
		return nil, nil
	}
	if len(tx.TxAttempts) != 1 || tx.TxAttempts[0].State != txmgrtypes.TxAttemptInProgress {
		return nil, fmt.Errorf("get_tx_in_progress: expected in_progress transaction %v to have exactly one unsent attempt. "+
			"Your database is in an inconsistent state and this node will not function correctly until the problem is resolved", tx.ID)
	}

	return tx, nil
}

// UpdateTxAttemptInProgressToBroadcast updates a transaction attempt from in_progress to broadcast.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxAttemptInProgressToBroadcast(
	ctx context.Context,
	tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	newAttemptState txmgrtypes.TxAttemptState,
) error {
	if tx.BroadcastAt == nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: unconfirmed transaction must have broadcast)at time")
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

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxAttemptInProgressToBroadcast(ctx, tx, attempt, newAttemptState); err != nil {
		return fmt.Errorf("update_tx_attempt_in_progress_to_broadcast: %w", err)
	}
	// Ensure that the tx state is updated to unconfirmed since this is a chain agnostic operation
	tx.State = TxUnconfirmed
	attempt.State = newAttemptState
	var found bool
	for i := 0; i < len(tx.TxAttempts); i++ {
		if tx.TxAttempts[i].ID == attempt.ID {
			tx.TxAttempts[i] = attempt
			found = true
		}
	}
	if !found {
		tx.TxAttempts = append(tx.TxAttempts, attempt)
		// NOTE(jtw): should this log a warning?
	}

	// remove the transaction from the inprogress map
	ms.inprogressLock.Lock()
	ms.inprogress[tx.FromAddress] = nil
	ms.inprogressLock.Unlock()

	// add the transaction to the unconfirmed map
	ms.unconfirmedLock.Lock()
	ms.unconfirmed[tx.FromAddress][tx.ID] = tx
	ms.unconfirmedLock.Unlock()

	return nil
}

// FindNextUnstartedTransactionFromAddress returns the next unstarted transaction for a given address.
// NOTE(jtw): method signature is different from most other signatures where the tx is passed in and updated
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindNextUnstartedTransactionFromAddress(_ context.Context, tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], fromAddress ADDR, chainID CHAIN_ID) error {
	if ms.chainID.String() != chainID.String() {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: %w", ErrInvalidChainID)
	}
	if _, ok := ms.unstarted[fromAddress]; !ok {
		return fmt.Errorf("find_next_unstarted_transaction_from_address: %w", ErrAddressNotFound)
	}
	// ensure that the address is not already busy with a transaction in progress
	ms.inprogressLock.RLock()
	if ms.inprogress[fromAddress] != nil {
		ms.inprogressLock.RUnlock()
		return fmt.Errorf("find_next_unstarted_transaction_from_address: address %s is already busy with a transaction in progress", fromAddress)
	}
	ms.inprogressLock.RUnlock()

	select {
	case tx = <-ms.unstarted[fromAddress]:
		return nil
	default:
		return ErrTxnNotFound
	}
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

	// Persist to persistent storage
	if err := ms.txStore.SaveReplacementInProgressAttempt(ctx, oldAttempt, replacementAttempt); err != nil {
		return fmt.Errorf("save_replacement_in_progress_attempt: %w", err)
	}

	// Update in memory store
	ms.inprogressLock.Lock()
	tx, ok := ms.inprogress[oldAttempt.Tx.FromAddress]
	if tx == nil || !ok {
		ms.inprogressLock.Unlock()
		return fmt.Errorf("save_replacement_in_progress_attempt: %w", ErrAddressNotFound)
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
		// NOTE(jtw): should this log a warning?
	}
	ms.inprogressLock.Unlock()

	return fmt.Errorf("save_replacement_in_progress_attempt: not implemented")
}

// UpdateTxFatalError updates a transaction to fatal_error.
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) UpdateTxFatalError(ctx context.Context, tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	if tx.State != TxInProgress {
		return fmt.Errorf("update_tx_fatal_error: can only transition to fatal_error from in_progress, transaction is currently %s", tx.State)
	}
	if !tx.Error.Valid {
		return fmt.Errorf("update_tx_fatal_error: expected error field to be set")
	}

	// Persist to persistent storage
	if err := ms.txStore.UpdateTxFatalError(ctx, tx); err != nil {
		return fmt.Errorf("update_tx_fatal_error: %w", err)
	}

	// Ensure that the tx state is updated to fatal_error since this is a chain agnostic operation
	tx.Sequence = nil
	tx.State = TxFatalError

	return fmt.Errorf("update_tx_fatal_error: not implemented")
}

// Close closes the InMemoryStore
func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) Close() {
	// Close the event recorder
	ms.txStore.Close()

	// Close all channels
	for _, ch := range ms.unstarted {
		close(ch)
	}

	// Clear all pending requests
	ms.pendingLock.Lock()
	clear(ms.pendingIdempotencyKeys)
	ms.pendingLock.Unlock()
	// Clear all unstarted transactions
	ms.inprogressLock.Lock()
	clear(ms.inprogress)
	ms.inprogressLock.Unlock()
	// Clear all unconfirmed transactions
	ms.unconfirmedLock.Lock()
	clear(ms.unconfirmed)
	ms.unconfirmedLock.Unlock()
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
	if _, ok := ms.unstarted[addr]; !ok {
		return fmt.Errorf("abandon: %w", ErrAddressNotFound)
	}
	// Mark all unstarted transactions as abandoned
	close(ms.unstarted[addr])
	for tx := range ms.unstarted[addr] {
		tx.State = TxFatalError
		tx.Sequence = nil
		tx.Error = null.NewString("abandoned", true)
	}
	// reset the unstarted channel
	ms.unstarted[addr] = make(chan *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], 100)

	ms.inprogressLock.Lock()
	if _, ok := ms.inprogress[addr]; !ok {
		ms.inprogressLock.Unlock()
		return fmt.Errorf("abandon: %w", ErrAddressNotFound)
	}
	// Mark all inprogress transactions as abandoned
	if tx, ok := ms.inprogress[addr]; ok {
		tx.State = TxFatalError
		tx.Sequence = nil
		tx.Error = null.NewString("abandoned", true)
	}
	ms.inprogress[addr] = nil
	ms.inprogressLock.Unlock()

	ms.unconfirmedLock.Lock()
	if _, ok := ms.unconfirmed[addr]; !ok {
		ms.unconfirmedLock.Unlock()
		return fmt.Errorf("abandon: %w", ErrAddressNotFound)
	}
	// Mark all unconfirmed transactions as abandoned
	for _, tx := range ms.unconfirmed[addr] {
		tx.State = TxFatalError
		tx.Sequence = nil
		tx.Error = null.NewString("abandoned", true)
	}
	ms.unconfirmed[addr] = nil
	ms.unconfirmedLock.Unlock()

	ms.pendingLock.Lock()
	// Mark all pending transactions as abandoned
	for _, tx := range ms.pendingIdempotencyKeys {
		if tx.FromAddress == addr {
			tx.State = TxFatalError
			tx.Sequence = nil
			tx.Error = null.NewString("abandoned", true)
		}
	}
	ms.pendingLock.Unlock()

	return nil
}

func (ms *InMemoryStore[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) sendTxToUnstartedQueue(tx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	// TODO(jtw); HANDLE PRUNING STEP

	select {
	// Add the request to the Unstarted channel to be processed by the Broadcaster
	case ms.unstarted[tx.FromAddress] <- &tx:
		// Persist to persistent storage

		ms.pendingLock.Lock()
		if tx.IdempotencyKey != nil {
			ms.pendingIdempotencyKeys[*tx.IdempotencyKey] = &tx
		}
		ms.pendingLock.Unlock()

		return nil
	default:
		// Return an error if the Manager Queue Capacity has been reached
		return fmt.Errorf("transaction manager queue capacity has been reached")
	}
}