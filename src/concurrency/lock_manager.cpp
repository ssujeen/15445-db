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
			lt[txn->GetTransactionId()] = std::chrono::system_clock::now();
		// add to the txn's shared lock set
		txn->GetSharedLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
	else
	{
		auto vec = iter->second;
		bool abort = false;
		// TODO: handle deadlock
		lc[rid].wait(ul, [&]
			{
				// only wait if someone acquired an exclusive lock
				// on this RID
				const bool cond_wait = ((vec.size() == 1))
					&& (vec[0].GetType() == LockType::LOCK_EXCLUSIVE);
				if (cond_wait)
				{
					// prevent deadlock before waiting
					const auto txn_id = txn->GetTransactionId();
					auto txn_timestamp = (lt.find(txn_id) != lt.end())
						? lt[txn_id] : std::chrono::system_clock::now();
					auto txn_id1 = vec[0].GetTxn()->GetTransactionId();
					assert (lt.find(txn_id1) != lt.end());
					abort = txn_timestamp > lt[txn_id1];
					// if we are going to wait, then record the timestamp
					if (!abort)
						lt[txn_id] = txn_timestamp;

					return abort;
				}
				else
				{
					return true;
				}
			});

		// no need to record timestamp if we are aborting
		if (abort)
			return false;

		// the timestamp of the txn is the timestamp of the first lock acquired
		// or the first wait
		if (lt.find(txn->GetTransactionId()) == lt.end())
			lt[txn->GetTransactionId()] = std::chrono::system_clock::now();
		vec.push_back(TxnLockStatus(LockType::LOCK_SHARED, txn));
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
		// add to the txn's exclusive lock set
		txn->GetExclusiveLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
	else
	{
		auto vec = iter->second;
		bool abort = false;
		lc[rid].wait(ul, [&]
			{
				// an exclusive lock is strict, we can only
				// acquire a lock if there is no contention

				const bool cond_wait = !vec.empty();
				if (!cond_wait)
					return true;
				// handle deadlock, if there exists any transaction
				// that has acquired a lock on this RID with a timestamp
				// older than this txn's timestamp, then we abort
				auto txn_id = txn->GetTransactionId();
				auto timestamp = (lt.find(txn_id) != lt.end())
					? lt[txn_id] : std::chrono::system_clock::now();
				auto it = std::find_if(vec.begin(), vec.end(), [&](TxnLockStatus &ts)
					{
						auto txn_id1 = ts.GetTxn()->GetTransactionId();

						assert(lt.find(txn_id1) != lt.end());

						return (timestamp > lt[txn_id1]);
					});
				abort = (it != vec.end());
				if (!abort)
				{
					// set the timestamp for the transaction if none exists
					if (lt.find(txn_id) == lt.end())
						lt[txn_id] = timestamp;
				}

				return abort;
			});

		if (abort)
			return false;

		vec.push_back(TxnLockStatus(LockType::LOCK_EXCLUSIVE, txn));
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

	auto vec = iter->second;
	bool abort = false;
	// TODO: handle deadlock
	lc[rid].wait(ul, [&]
		{
			// only acquire the lock if the only lock left
			// is a shared lock that is owned by this txn
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
			auto timestamp = std::chrono::system_clock::now();
			auto it = std::find_if(vec.begin(), vec.end(), [&](TxnLockStatus &ts)
				{
					auto txn_id1 = ts.GetTxn()->GetTransactionId();

					assert(lt.find(txn_id1) != lt.end());

					return (timestamp > lt[txn_id1]);
				});
			abort = (it != vec.end());
			if (!abort)
			{
				// set the timestamp for the transaction if none exists
				if (lt.find(txn_id) == lt.end())
					lt[txn_id] = timestamp;
			}

			return abort;
		});

	if (abort)
		return false;
	// we need to remove the RID from the transaction's sharedlock set
	txn->GetSharedLockSet()->erase(rid);
	// also remove from the lock manager. at this point the vector has *one* elt
	// which is the elt we need to remove
	vec.pop_back();
	// add to the lock manager as exclusive lock and also
	// to the transaction's exclusive lock set
	vec.push_back(TxnLockStatus(LockType::LOCK_EXCLUSIVE, txn));
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
		lt.erase(txn->GetTransactionId());

	// we need to remove the txn from the lock manager
	auto iter = lm.find(rid);
	assert (iter != lm.end());
	auto vec = iter->second;

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
	return true;
}

} // namespace cmudb
