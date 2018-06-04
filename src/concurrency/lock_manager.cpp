/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"

namespace cmudb {

void LockManager::check(Transaction* txn, RID const& rid)
{
	// a RID shouldn't be in both the txn's sharedlock set
	// and also in the txn's exclusive lock set

	const bool pred = txn->GetSharedLockSet()->find(rid)
		!= txn->GetSharedLockSet()->end();
	const bool pred1 = txn->GetExclusiveLockSet()->find(rid)
		!= txn->GetExclusiveLockSet()->end();
	const bool final_pred = (pred && pred1);

	if (final_pred)
		assert(false);
}

using namespace std::chrono;
uint64_t LockManager::get_timestamp()
{
	const auto now = system_clock::now();
	const auto now_ns = time_point_cast<nanoseconds>(now);
	const auto time_since = now_ns.time_since_epoch();
	const auto val = duration_cast<nanoseconds>(time_since);
	const uint64_t value = val.count();

	return value;
}

bool LockManager::LockShared(Transaction *txn, const RID &rid)
{
	// locks can happen only in the GROWING phase
	assert (txn->GetState() == TransactionState::GROWING);
	// protect concurrent access to the unordered_map
	std::unique_lock<std::mutex> ul (mtx);
	auto iter = lm.find(rid);
	if (iter == lm.end())
	{
		// if RID is not present, then no one else is contended
		// for the RID so, we can acquire the lock
		lm[rid].push_back(TxnLockStatus(LockType::LOCK_SHARED, txn));

		// the timestamp of the txn is the timestamp of the first lock acquired
		if (lt.find(txn->GetTransactionId()) == lt.end())
			lt[txn->GetTransactionId()] = get_timestamp();
		// add to the txn's shared lock set
		txn->GetSharedLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
	else
	{
		bool abort = false;
		lc[rid].wait(ul, [&]
			{
				// only wait if someone acquired an exclusive lock
				// on this RID
				auto iter = lm.find(rid);
				if (iter == lm.end())
				{
					// this path will only be taken if the txn has
					// already waited, so don't update the timestamp
					return true;
				}
				auto &vec = iter->second;
				const bool cond_wait = ((vec.size() == 1)
					&& (vec[0].GetType() == LockType::LOCK_EXCLUSIVE));

				if (!cond_wait)
					return true;
				// prevent deadlock before waiting
				const auto txn_id = txn->GetTransactionId();
				auto txn_timestamp = (lt.find(txn_id) != lt.end())
					? lt[txn_id] : get_timestamp();
				auto txn_id1 = vec[0].GetTxn()->GetTransactionId();
				assert (lt.find(txn_id1) != lt.end());
				abort = txn_timestamp > lt[txn_id1];
				// there is no need to record the timestamp when we wait
				// the reason is due to the deadlock prevention policy
				// ie wait-die. we only wait if our timestamp is older than
				// the timestamp of the txn holding the lock and since timestamps
				// are recorded at the time of the first lock acquisition, the waiting
				// transaction already must hold some other lock on a different RID
				// for even having a scenario where its timestamp is older than the one
				// which is holding the lock.

				return abort;
			});

		// no need to record timestamp if we are aborting
		if (abort)
		{
			// failed transaction should implicitly set the txn state to aborted
			txn->SetState(TransactionState::ABORTED);
			return false;
		}

		// the timestamp of the txn is the timestamp of the first lock acquired
		// we need to update the timestamp here because it is possible for thie
		// code to get hit without a wait.
		// for eg) consider the following scenario
		// txnA acquires a shared lock on RID(A), txnB tries to acquire
		// a lock on RID(A), lm[rid] will be a vector of size 1 for txnB.
		// in the else case, cond_wait will be false, so it will return without
		// waiting. the core idea is simple, if there is a case where the txn
		// may not wait, we need to update the timestamp else don't have to do it
		if (lt.find(txn->GetTransactionId()) == lt.end())
			lt[txn->GetTransactionId()] = get_timestamp();
		lm[rid].push_back(TxnLockStatus(LockType::LOCK_SHARED, txn));
		txn->GetSharedLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid)
{
	// locks can only happen in the growing phase
	assert (txn->GetState() == TransactionState::GROWING);

	// protect concurrent access to the unordered_map
	std::unique_lock<std::mutex> ul (mtx);
	auto iter = lm.find(rid);
	if (iter == lm.end())
	{
		// if RID is not present, then no one else is contended
		// for the RID so, we can acquire the lock
		lm[rid].push_back(TxnLockStatus(LockType::LOCK_EXCLUSIVE, txn));
		// the timestamp of the txn is the timestamp of the first lock acquired
		if (lt.find(txn->GetTransactionId()) == lt.end())
			lt[txn->GetTransactionId()] = get_timestamp();
		// add to the txn's exclusive lock set
		txn->GetExclusiveLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
	else
	{
		bool abort = false;
		lc[rid].wait(ul, [&]
			{
				// an exclusive lock is strict, we can only
				// acquire a lock if there is no contention

				auto iter = lm.find(rid);
				if (iter == lm.end())
				{
					// there is no need to update the timestamp here.
					// the reason is subtle. if there was no contention, we would
					// have acquired the lock in the 'if' case above
					// now that we are here, we can wait if we had already acquired
					// another lock (because of the deadlock prevention policy)
					// so there is no need to update the timestamp here
					return true;
				}

				// at this point the vector has to be non-empty
				// because when the vector becomes empty, we remove it from the map
				auto &vec = iter->second;
				assert (vec.size() != 0);
				// handle deadlock, if there exists any transaction
				// that has acquired a lock on this RID with a timestamp
				// older than this txn's timestamp, then we abort
				auto txn_id = txn->GetTransactionId();
				auto timestamp = (lt.find(txn_id) != lt.end())
					? lt[txn_id] : get_timestamp();
				auto it = std::find_if(vec.begin(), vec.end(), [&](TxnLockStatus &ts)
					{
						auto txn_id1 = ts.GetTxn()->GetTransactionId();

						assert(lt.find(txn_id1) != lt.end());

						return (timestamp > lt[txn_id1]);
					});
				abort = (it != vec.end());
				// if we are waiting, we don't need to update timestamp
				// see comments in LockShared

				return abort;
			});

		if (abort)
		{
			txn->SetState(TransactionState::ABORTED);
			return false;
		}

		// no need to update timestamp here since we would only get
		// here by waiting, if the else case is taken at the top, we must have
		// waited atleast once
		lm[rid].push_back(TxnLockStatus(LockType::LOCK_EXCLUSIVE, txn));
		txn->GetExclusiveLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid)
{
	// lock upgrades can happen only in the GROWING phase
	assert (txn->GetState() == TransactionState::GROWING);
	// protect concurrent access to the unordered_map
	std::unique_lock<std::mutex> ul (mtx);
	auto iter = lm.find(rid);

	// there should be already a shared lock on RID
	assert (iter != lm.end());
	bool valid = false;
	for (auto elem : iter->second)
	{
		if ((elem.GetType() == LockType::LOCK_SHARED)
			&& (elem.GetTxn()->GetTransactionId() == txn->GetTransactionId()))
		{
			valid = true;
			break;
		}
	}

	assert (valid == true);

	bool abort = false;
	lc[rid].wait(ul, [&]
		{
			// only acquire the lock if the only lock left
			// is a shared lock that is owned by this txn
			auto iter = lm.find(rid);
			assert (iter != lm.end());
			auto &vec = iter->second;
			const auto txId = txn->GetTransactionId();
			const bool cond_success = ((vec.size() == 1))
				&& (vec[0].GetType() == LockType::LOCK_SHARED)
				&& (vec[0].GetTxn()->GetTransactionId() == txId);

			if (cond_success)
				return true;
			// deadlock prevention
			auto txn_id = txn->GetTransactionId();
			// timestamp should be there, because we must have
			// already acquired a shared lock
			assert (lt.find(txn_id) != lt.end());
			auto timestamp = lt[txn_id];
			auto it = std::find_if(vec.begin(), vec.end(), [&](TxnLockStatus &ts)
				{
					auto txn_id1 = ts.GetTxn()->GetTransactionId();

					assert(lt.find(txn_id1) != lt.end());

					return (timestamp > lt[txn_id1]);
				});
			abort = (it != vec.end());
			return abort;
		});

	if (abort)
	{
		txn->SetState(TransactionState::ABORTED);
		return false;
	}
	// we need to remove the RID from the transaction's sharedlock set
	txn->GetSharedLockSet()->erase(rid);
	// also remove from the lock manager. at this point the vector has *one* elt
	// which is the elt we need to remove
	lm[rid].pop_back();
	// no need to enter into the timestamp map because this txn would have
	// already acquired a shared lock which would enter into the timestamp
	// map if an entry doesn't exist already
	// add to the lock manager as exclusive lock and also
	// to the transaction's exclusive lock set
	lm[rid].push_back(TxnLockStatus(LockType::LOCK_EXCLUSIVE, txn));
	txn->GetExclusiveLockSet()->insert(rid);
	check(txn, rid);
	ul.unlock();
	return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid)
{
	// switch to the SHRINKING phase during the first unlock
	if (txn->GetState() == TransactionState::GROWING)
		txn->SetState(TransactionState::SHRINKING);

	// rid would be present in either the shared lock set
	// or the exclusive lock set, remove from it
	check(txn, rid);
	txn->GetSharedLockSet()->erase(rid);
	txn->GetExclusiveLockSet()->erase(rid);

	const bool shared_empty = txn->GetSharedLockSet()->empty();
	const bool exclusive_empty = txn->GetExclusiveLockSet()->empty();
	std::lock_guard<std::mutex> lg(mtx);

	// if this is the last lock held by the transaction, then
	// remove the transaction timestamp
	if (shared_empty && exclusive_empty)
	{
		lt.erase(txn->GetTransactionId());
	}

	// we need to remove the txn from the lock manager
	auto iter = lm.find(rid);
	assert (iter != lm.end());
	auto &vec = iter->second;

	for (auto it = vec.begin(); it != vec.end();)
	{
		if (it->GetTxn()->GetTransactionId() == txn->GetTransactionId())
		{
			// there will never be a *valid* case where the txn holds more than one
			// lock on the same rid, so break after handling the first one.
			it = vec.erase(it);
			break;
		}
		else
		{
			it++;
		}
	}

	// if the vector is empty ie there is no lock on the rid, then
	// remove the key from the unordered_map
	if (vec.empty())
		lm.erase(rid);

	// notify all the waiting threads if any
	lc[rid].notify_all();

	// do the check for the strict 2PL at the end to prevent deadlocks
	if (strict_2PL_)
	{
		// in strict 2PL, unlock only after the transaction commits
		if ((txn->GetState() != TransactionState::COMMITTED)
			&& (txn->GetState() != TransactionState::ABORTED))
		{
			txn->SetState(TransactionState::ABORTED);
			return false;
		}
	}

	return true;
}

} // namespace cmudb
