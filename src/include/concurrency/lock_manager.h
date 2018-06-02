/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <vector>
#include <mutex>
#include <unordered_map>
#include <cassert>
#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

enum class LockType
{
	LOCK_SHARED = 0, LOCK_EXCLUSIVE
};

typedef struct TxnLockStatus
{
	LockType type_;
	Transaction* txn_;

	TxnLockStatus(LockType type, Transaction* txn)
		: type_(type), txn_(txn)
	{
	}

	LockType GetType()
	{
		return type_;
	}

	Transaction* GetTxn()
	{
		return txn_;
	}
} TxnLockStatus;

class LockManager {

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  bool strict_2PL_;

  // vector of locks held per RID (for exclusive lock there will be only
  // one entry per RID, for shared locks there could be more
  // and hence we need a vector)
  std::unordered_map <RID, std::vector<TxnLockStatus>> lm;
  // one condition variable per RID
  std::unordered_map <RID, std::condition_variable> lc;
  std::mutex mtx;

  void check(Transaction*, RID const&);
};

} // namespace cmudb
