/**
 * transaction_manager.cpp
 *
 */
#include "concurrency/transaction_manager.h"
#include "table/table_heap.h"

#include <cassert>
namespace cmudb {

Transaction *TransactionManager::Begin() {
  Transaction *txn = new Transaction(next_txn_id_++);

  if (ENABLE_LOGGING) {
	  // begin has no prev_lsn
	  LogRecord lg(txn->GetTransactionId(), INVALID_LSN, LogRecordType::BEGIN);
	  const lsn_t prev_lsn = log_manager_->AppendLogRecord(lg);
	  assert (txn->GetPrevLSN() == INVALID_LSN);
	  txn->SetPrevLSN(prev_lsn);
  }

  return txn;
}

void TransactionManager::Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);
  // truly delete before commit
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      // this also release the lock when holding the page latch
      table->ApplyDelete(item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
	  const lsn_t prev_lsn = txn->GetPrevLSN();
	  assert (prev_lsn != INVALID_LSN);
	  LogRecord lg(txn->GetTransactionId(), prev_lsn, LogRecordType::COMMIT);
	  // commit is the last step in the transaction, we don't need to update
	  // the txn's prev lsn at this point
	  log_manager_->AppendLogRecord(lg);

	  // add a promise, and wait for LOG_TIMEOUT or buffer flush
	  // the idea is not to force a log flush thereby saving a fsync
	  // instead wait for a log flush event to happen like timeout/buffer full
	  while (true)
	  {
	  	std::promise<lsn_t> pr;
	  	std::future<lsn_t> fut = pr.get_future();
	  	log_manager_->add_promise_lsn(pr);

		lsn_t last_lsn = fut.get();
		// if our log record is in the disk, we can move forward
		if (last_lsn >= prev_lsn)
			break;
	  }
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}

void TransactionManager::Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);
  // rollback before releasing lock
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back();
    auto table = item.table_;
    if (item.wtype_ == WType::DELETE) {
      LOG_DEBUG("rollback delete");
      table->RollbackDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::INSERT) {
      LOG_DEBUG("rollback insert");
      table->ApplyDelete(item.rid_, txn);
    } else if (item.wtype_ == WType::UPDATE) {
      LOG_DEBUG("rollback update");
      table->UpdateTuple(item.tuple_, item.rid_, txn);
    }
    write_set->pop_back();
  }
  write_set->clear();

  if (ENABLE_LOGGING) {
	  const lsn_t prev_lsn = txn->GetPrevLSN();
	  assert (prev_lsn != INVALID_LSN);
	  LogRecord lg(txn->GetTransactionId(), prev_lsn, LogRecordType::ABORT);
	  // abort is the last step in the transaction, we don't need to update
	  // the txn's prev lsn at this point
	  log_manager_->AppendLogRecord(lg);
  }

  // release all the lock
  std::unordered_set<RID> lock_set;
  for (auto item : *txn->GetSharedLockSet())
    lock_set.emplace(item);
  for (auto item : *txn->GetExclusiveLockSet())
    lock_set.emplace(item);
  // release all the lock
  for (auto locked_rid : lock_set) {
    lock_manager_->Unlock(txn, locked_rid);
  }
}
} // namespace cmudb
