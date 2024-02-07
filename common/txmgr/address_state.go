package txmgr

import (
	"fmt"
	"sync"
	"time"

	"github.com/smartcontractkit/chainlink-common/pkg/logger"
	feetypes "github.com/smartcontractkit/chainlink/v2/common/fee/types"
	txmgrtypes "github.com/smartcontractkit/chainlink/v2/common/txmgr/types"
	"github.com/smartcontractkit/chainlink/v2/common/types"
	"gopkg.in/guregu/null.v4"
)

// AddressState is the state of a given from address
type AddressState[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
] struct {
	lggr        logger.SugaredLogger
	chainID     CHAIN_ID
	fromAddress ADDR

	sync.RWMutex
	idempotencyKeyToTx map[string]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	unstarted          *TxPriorityQueue[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]
	inprogress         *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	// NOTE: currently the unconfirmed map's key is the transaction ID that is assigned via the postgres DB
	unconfirmed             map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	confirmedMissingReceipt map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	confirmed               map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	allTransactions         map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	fatalErrored            map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
}

// NewAddressState returns a new AddressState instance
func NewAddressState[
	CHAIN_ID types.ID,
	ADDR, TX_HASH, BLOCK_HASH types.Hashable,
	R txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
	SEQ types.Sequence,
	FEE feetypes.Fee,
](
	lggr logger.SugaredLogger,
	chainID CHAIN_ID,
	fromAddress ADDR,
	maxUnstarted int,
	txs []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) (*AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE], error) {
	// Count the number of transactions in each state to reduce the number of map resizes
	counts := map[txmgrtypes.TxState]int{
		TxUnstarted:               0,
		TxInProgress:              0,
		TxUnconfirmed:             0,
		TxConfirmedMissingReceipt: 0,
		TxConfirmed:               0,
		TxFatalError:              0,
	}
	for _, tx := range txs {
		counts[tx.State]++
	}

	as := AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]{
		lggr:        lggr,
		chainID:     chainID,
		fromAddress: fromAddress,

		idempotencyKeyToTx:      make(map[string]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs)),
		unstarted:               NewTxPriorityQueue[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE](maxUnstarted),
		inprogress:              nil,
		unconfirmed:             make(map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], counts[TxUnconfirmed]),
		confirmedMissingReceipt: make(map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], counts[TxConfirmedMissingReceipt]),
		confirmed:               make(map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], counts[TxConfirmed]),
		allTransactions:         make(map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], len(txs)),
		fatalErrored:            make(map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], counts[TxFatalError]),
	}

	// Load all transactions supplied
	for i := 0; i < len(txs); i++ {
		tx := txs[i]
		switch tx.State {
		case TxUnstarted:
			as.unstarted.AddTx(&tx)
		case TxInProgress:
			as.inprogress = &tx
		case TxUnconfirmed:
			as.unconfirmed[tx.ID] = &tx
		case TxConfirmedMissingReceipt:
			as.confirmedMissingReceipt[tx.ID] = &tx
		case TxConfirmed:
			as.confirmed[tx.ID] = &tx
		case TxFatalError:
			as.fatalErrored[tx.ID] = &tx
		}
		as.allTransactions[tx.ID] = &tx
		if tx.IdempotencyKey != nil {
			as.idempotencyKeyToTx[*tx.IdempotencyKey] = &tx
		}
	}

	return &as, nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) close() {
	clear(as.idempotencyKeyToTx)

	as.unstarted.Close()
	as.unstarted = nil
	as.inprogress = nil

	clear(as.unconfirmed)
	clear(as.confirmedMissingReceipt)
	clear(as.confirmed)
	clear(as.allTransactions)
	clear(as.fatalErrored)

	as.idempotencyKeyToTx = nil
	as.unconfirmed = nil
	as.confirmedMissingReceipt = nil
	as.confirmed = nil
	as.allTransactions = nil
	as.fatalErrored = nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) CountTransactionsByState(txState txmgrtypes.TxState) int {
	as.RLock()
	defer as.RUnlock()

	switch txState {
	case TxUnstarted:
		return as.unstarted.Len()
	case TxInProgress:
		if as.inprogress != nil {
			return 1
		}
	case TxUnconfirmed:
		return len(as.unconfirmed)
	case TxConfirmedMissingReceipt:
		return len(as.confirmedMissingReceipt)
	case TxConfirmed:
		return len(as.confirmed)
	case TxFatalError:
		return len(as.fatalErrored)
	}

	return 0
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FindTxWithIdempotencyKey(key string) *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	as.RLock()
	defer as.RUnlock()

	return as.idempotencyKeyToTx[key]
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) ApplyToTxsByState(
	txStates []txmgrtypes.TxState,
	fn func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]),
	txIDs ...int64,
) {
	as.Lock()
	defer as.Unlock()

	// if txStates is empty then apply the filter to only the as.allTransactions map
	if len(txStates) == 0 {
		as.applyToTxs(as.allTransactions, fn, txIDs...)
		return
	}

	for _, txState := range txStates {
		switch txState {
		case TxInProgress:
			if as.inprogress != nil {
				fn(as.inprogress)
			}
		case TxUnconfirmed:
			as.applyToTxs(as.unconfirmed, fn, txIDs...)
		case TxConfirmedMissingReceipt:
			as.applyToTxs(as.confirmedMissingReceipt, fn, txIDs...)
		case TxConfirmed:
			as.applyToTxs(as.confirmed, fn, txIDs...)
		case TxFatalError:
			as.applyToTxs(as.fatalErrored, fn, txIDs...)
		}
	}
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) applyToTxs(
	txIDsToTx map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	fn func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]),
	txIDs ...int64,
) {
	// if txIDs is not empty then only apply the filter to those transactions
	if len(txIDs) > 0 {
		for _, txID := range txIDs {
			tx := txIDsToTx[txID]
			if tx != nil {
				fn(tx)
			}
		}
		return
	}

	// if txIDs is empty then apply the filter to all transactions
	for _, tx := range txIDsToTx {
		fn(tx)
	}
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FetchTxAttempts(
	txStates []txmgrtypes.TxState,
	filter func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool,
	txIDs ...int64,
) []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	as.RLock()
	defer as.RUnlock()

	// if txStates is empty then apply the filter to only the as.allTransactions map
	if len(txStates) == 0 {
		return as.fetchTxAttemptsFromStorage(as.allTransactions, filter, txIDs...)
	}

	var txAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, txState := range txStates {
		switch txState {
		case TxInProgress:
			if as.inprogress != nil && filter(as.inprogress) {
				txAttempts = append(txAttempts, as.inprogress.TxAttempts...)
			}
		case TxUnconfirmed:
			txAttempts = append(txAttempts, as.fetchTxAttemptsFromStorage(as.unconfirmed, filter, txIDs...)...)
		case TxConfirmedMissingReceipt:
			txAttempts = append(txAttempts, as.fetchTxAttemptsFromStorage(as.confirmedMissingReceipt, filter, txIDs...)...)
		case TxConfirmed:
			txAttempts = append(txAttempts, as.fetchTxAttemptsFromStorage(as.confirmed, filter, txIDs...)...)
		case TxFatalError:
			txAttempts = append(txAttempts, as.fetchTxAttemptsFromStorage(as.fatalErrored, filter, txIDs...)...)
		}
	}

	return txAttempts
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) fetchTxAttemptsFromStorage(
	txIDsToTx map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	filter func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool,
	txIDs ...int64,
) []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	as.RLock()
	defer as.RUnlock()

	var txAttempts []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	// if txIDs is not empty then only apply the filter to those transactions
	if len(txIDs) > 0 {
		for _, txID := range txIDs {
			tx := txIDsToTx[txID]
			if tx != nil && filter(tx) {
				txAttempts = append(txAttempts, tx.TxAttempts...)
			}
		}
		return txAttempts
	}

	// if txIDs is empty then apply the filter to all transactions
	for _, tx := range txIDsToTx {
		if filter(tx) {
			txAttempts = append(txAttempts, tx.TxAttempts...)
		}
	}

	return txAttempts
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) FetchTxs(
	txStates []txmgrtypes.TxState,
	filter func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool,
	txIDs ...int64,
) []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	as.RLock()
	defer as.RUnlock()

	// if txStates is empty then apply the filter to only the as.allTransactions map
	if len(txStates) == 0 {
		return as.fetchTxs(as.allTransactions, filter, txIDs...)
	}

	var txs []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	for _, txState := range txStates {
		switch txState {
		case TxInProgress:
			if as.inprogress != nil && filter(as.inprogress) {
				txs = append(txs, *as.inprogress)
			}
		case TxUnconfirmed:
			txs = append(txs, as.fetchTxs(as.unconfirmed, filter, txIDs...)...)
		case TxConfirmedMissingReceipt:
			txs = append(txs, as.fetchTxs(as.confirmedMissingReceipt, filter, txIDs...)...)
		case TxConfirmed:
			txs = append(txs, as.fetchTxs(as.confirmed, filter, txIDs...)...)
		case TxFatalError:
			txs = append(txs, as.fetchTxs(as.fatalErrored, filter, txIDs...)...)
		}
	}

	return txs
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) fetchTxs(
	txIDsToTx map[int64]*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	filter func(*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) bool,
	txIDs ...int64,
) []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE] {
	as.RLock()
	defer as.RUnlock()

	var txs []txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]
	// if txIDs is not empty then only apply the filter to those transactions
	if len(txIDs) > 0 {
		for _, txID := range txIDs {
			tx := txIDsToTx[txID]
			if tx != nil && filter(tx) {
				txs = append(txs, *tx)
			}
		}
		return txs
	}

	// if txIDs is empty then apply the filter to all transactions
	for _, tx := range txIDsToTx {
		if filter(tx) {
			txs = append(txs, *tx)
		}
	}

	return txs
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) PruneUnstartedTxQueue(ids []int64) {
	as.Lock()
	defer as.Unlock()

	txs := as.unstarted.PruneByTxIDs(ids)
	as.deleteTxs(txs...)
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) DeleteTxs(txs ...txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
	as.Lock()
	defer as.Unlock()

	as.deleteTxs(txs...)
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) deleteTxs(txs ...txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
	for _, tx := range txs {
		if tx.IdempotencyKey != nil {
			delete(as.idempotencyKeyToTx, *tx.IdempotencyKey)
		}
		txID := tx.ID
		if as.inprogress != nil && as.inprogress.ID == txID {
			as.inprogress = nil
		}
		delete(as.allTransactions, txID)
		delete(as.unconfirmed, txID)
		delete(as.confirmedMissingReceipt, txID)
		delete(as.allTransactions, txID)
		delete(as.confirmed, txID)
		delete(as.fatalErrored, txID)
	}
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) PeekNextUnstartedTx() (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	as.RLock()
	defer as.RUnlock()

	tx := as.unstarted.PeekNextTx()
	if tx == nil {
		return nil, fmt.Errorf("peek_next_unstarted_tx: %w (address: %s)", ErrTxnNotFound, as.fromAddress)
	}

	return tx, nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) PeekInProgressTx() (*txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], error) {
	as.RLock()
	defer as.RUnlock()

	tx := as.inprogress
	if tx == nil {
		return nil, fmt.Errorf("peek_in_progress_tx: %w (address: %s)", ErrTxnNotFound, as.fromAddress)
	}

	return tx, nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) AddTxToUnstarted(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	as.Lock()
	defer as.Unlock()

	if as.unstarted.Len() >= as.unstarted.Cap() {
		return fmt.Errorf("move_tx_to_unstarted: address %s unstarted queue capactiry has been reached", as.fromAddress)
	}

	as.unstarted.AddTx(tx)
	as.allTransactions[tx.ID] = tx
	if tx.IdempotencyKey != nil {
		as.idempotencyKeyToTx[*tx.IdempotencyKey] = tx
	}

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveUnstartedToInProgress(
	etx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	txAttempt *txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) error {
	as.Lock()
	defer as.Unlock()

	if as.inprogress != nil {
		return fmt.Errorf("move_unstarted_to_in_progress: address %s already has a transaction in progress", as.fromAddress)
	}

	tx := as.unstarted.RemoveTxByID(etx.ID)
	if tx == nil {
		return fmt.Errorf("move_unstarted_to_in_progress: no unstarted transaction to move to in_progress")
	}
	tx.State = TxInProgress
	as.inprogress = tx

	newTxAttempts := []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{}
	affectedTxAttempts := 0
	for i := 0; i < len(tx.TxAttempts); i++ {
		// Remove any previous attempts that are in a fatal error state which share the same hash
		if tx.TxAttempts[i].Hash == txAttempt.Hash &&
			tx.State == TxFatalError && tx.Error == null.NewString("abandoned", true) {
			affectedTxAttempts++
			continue
		}
		newTxAttempts = append(newTxAttempts, tx.TxAttempts[i])
	}
	if affectedTxAttempts > 0 {
		as.lggr.Debugf("Replacing abandoned tx with tx hash %s with tx_id=%d with identical tx hash", txAttempt.Hash, txAttempt.TxID)
	}
	tx.TxAttempts = append(newTxAttempts, *txAttempt)
	tx.Sequence = etx.Sequence
	tx.BroadcastAt = etx.BroadcastAt
	tx.InitialBroadcastAt = etx.InitialBroadcastAt

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveConfirmedMissingReceiptToUnconfirmed(
	txID int64,
) error {
	as.Lock()
	defer as.Unlock()

	tx, ok := as.confirmedMissingReceipt[txID]
	if !ok || tx == nil {
		return fmt.Errorf("move_confirmed_missing_receipt_to_unconfirmed: no confirmed_missing_receipt transaction with ID %d: %w", txID, ErrTxnNotFound)
	}

	tx.State = TxUnconfirmed
	as.unconfirmed[tx.ID] = tx
	delete(as.confirmedMissingReceipt, tx.ID)

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveInProgressToUnconfirmed(
	txAttempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
) error {
	as.Lock()
	defer as.Unlock()

	tx := as.inprogress
	if tx == nil {
		return fmt.Errorf("move_in_progress_to_unconfirmed: no transaction in progress")
	}

	txAttempt.TxID = tx.ID
	txAttempt.State = txmgrtypes.TxAttemptBroadcast
	tx.State = TxUnconfirmed
	tx.TxAttempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{txAttempt}

	as.unconfirmed[tx.ID] = tx
	as.inprogress = nil

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveUnconfirmedToConfirmed(
	receipt txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH],
) error {
	as.Lock()
	defer as.Unlock()

	for _, tx := range as.unconfirmed {
		if tx.TxAttempts == nil {
			continue
		}
		for i := 0; i < len(tx.TxAttempts); i++ {
			txAttempt := tx.TxAttempts[i]
			if receipt.GetTxHash() == txAttempt.Hash {
				// TODO(jtw): not sure how to set blocknumber, transactionindex, and receipt on conflict
				txAttempt.Receipts = []txmgrtypes.ChainReceipt[TX_HASH, BLOCK_HASH]{receipt}
				txAttempt.State = txmgrtypes.TxAttemptBroadcast
				if txAttempt.BroadcastBeforeBlockNum == nil {
					blockNum := receipt.GetBlockNumber().Int64()
					txAttempt.BroadcastBeforeBlockNum = &blockNum
				}

				tx.State = TxConfirmed
				return nil
			}
		}
	}

	return fmt.Errorf("move_unconfirmed_to_confirmed: no unconfirmed transaction with receipt %v: %w", receipt, ErrTxnNotFound)
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveUnstartedToFatalError(
	etx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	txError null.String,
) error {
	as.Lock()
	defer as.Unlock()

	tx := as.unstarted.RemoveTxByID(etx.ID)
	if tx == nil {
		return fmt.Errorf("move_unstarted_to_fatal_error: no unstarted transaction with ID %d", etx.ID)
	}

	tx.State = TxFatalError
	tx.Sequence = nil
	tx.TxAttempts = nil
	tx.InitialBroadcastAt = nil
	tx.Error = txError
	as.fatalErrored[tx.ID] = tx

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveInProgressToFatalError(txError null.String) error {
	as.Lock()
	defer as.Unlock()

	tx := as.inprogress
	if tx == nil {
		return fmt.Errorf("move_in_progress_to_fatal_error: no transaction in progress")
	}

	tx.State = TxFatalError
	tx.Sequence = nil
	tx.TxAttempts = nil
	tx.InitialBroadcastAt = nil
	tx.Error = txError
	as.fatalErrored[tx.ID] = tx
	as.inprogress = nil

	return nil
}
func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveConfirmedMissingReceiptToFatalError(
	etx txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE],
	txError null.String,
) error {
	as.Lock()
	defer as.Unlock()

	tx, ok := as.confirmedMissingReceipt[etx.ID]
	if !ok || tx == nil {
		return fmt.Errorf("move_confirmed_missing_receipt_to_fatal_error: no confirmed_missing_receipt transaction with ID %d: %w", etx.ID, ErrTxnNotFound)
	}

	tx.State = TxFatalError
	tx.Sequence = nil
	tx.TxAttempts = nil
	tx.InitialBroadcastAt = nil
	tx.Error = txError
	as.fatalErrored[tx.ID] = tx
	delete(as.confirmedMissingReceipt, tx.ID)

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveUnconfirmedToConfirmedMissingReceipt(attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], broadcastAt time.Time) error {
	as.Lock()
	defer as.Unlock()

	tx, ok := as.unconfirmed[attempt.TxID]
	if !ok || tx == nil {
		return fmt.Errorf("move_unconfirmed_to_confirmed_missing_receipt: no unconfirmed transaction with ID %d: %w", attempt.TxID, ErrTxnNotFound)
	}
	if tx.BroadcastAt.Before(broadcastAt) {
		tx.BroadcastAt = &broadcastAt
	}
	tx.State = TxConfirmedMissingReceipt
	tx.TxAttempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{attempt}
	tx.TxAttempts[0].State = txmgrtypes.TxAttemptBroadcast

	as.confirmedMissingReceipt[tx.ID] = tx
	delete(as.unconfirmed, tx.ID)

	return nil
}
func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveInProgressToConfirmedMissingReceipt(attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE], broadcastAt time.Time) error {
	as.Lock()
	defer as.Unlock()

	tx := as.inprogress
	if tx == nil {
		return fmt.Errorf("move_in_progress_to_confirmed_missing_receipt: no transaction in progress")
	}
	if tx.BroadcastAt.Before(broadcastAt) {
		tx.BroadcastAt = &broadcastAt
	}
	tx.State = TxConfirmedMissingReceipt
	tx.TxAttempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{attempt}
	tx.TxAttempts[0].State = txmgrtypes.TxAttemptBroadcast

	as.confirmedMissingReceipt[tx.ID] = tx
	as.inprogress = nil

	return nil
}
func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) MoveConfirmedToUnconfirmed(attempt txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) error {
	as.Lock()
	defer as.Unlock()

	if attempt.State != txmgrtypes.TxAttemptBroadcast {
		return fmt.Errorf("move_confirmed_to_unconfirmed: attempt must be in broadcast state")
	}

	tx, ok := as.confirmed[attempt.TxID]
	if !ok || tx == nil {
		return fmt.Errorf("move_confirmed_to_unconfirmed: no confirmed transaction with ID %d: %w", attempt.TxID, ErrTxnNotFound)
	}
	tx.State = TxUnconfirmed

	// Delete the receipt from the attempt
	attempt.Receipts = nil
	// Reset the broadcast information for the attempt
	attempt.State = txmgrtypes.TxAttemptInProgress
	attempt.BroadcastBeforeBlockNum = nil
	tx.TxAttempts = []txmgrtypes.TxAttempt[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]{attempt}

	as.unconfirmed[tx.ID] = tx
	delete(as.confirmed, tx.ID)

	return nil
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) abandon() {
	as.Lock()
	defer as.Unlock()

	for as.unstarted.Len() > 0 {
		tx := as.unstarted.RemoveNextTx()
		as.abandonTx(tx)
	}

	if as.inprogress != nil {
		tx := as.inprogress
		as.abandonTx(tx)
		as.inprogress = nil
	}
	for _, tx := range as.unconfirmed {
		as.abandonTx(tx)
	}

	clear(as.unconfirmed)
}

func (as *AddressState[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, R, SEQ, FEE]) abandonTx(tx *txmgrtypes.Tx[CHAIN_ID, ADDR, TX_HASH, BLOCK_HASH, SEQ, FEE]) {
	if tx == nil {
		return
	}

	tx.State = TxFatalError
	tx.Sequence = nil
	tx.Error = null.NewString("abandoned", true)

	as.fatalErrored[tx.ID] = tx
}
