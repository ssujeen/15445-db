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
		// add to the txn's shared lock set
		txn->GetSharedLockSet()->insert(rid);
		check(txn, rid);
		ul.unlock();
		return true;
	}
	else
	{
		auto vec = iter->second;
		// TODO: handle deadlock
		lc[rid].wait(ul, [&]
			{
				// only wait if someone acquired an exclusive lock
				// on this RID
				const bool cond_wait = ((vec.size() == 1))
					&& (vec[0].GetType() == LockType::LOCK_EXCLUSIVE);

				return !cond_wait;
			});

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
		// TODO: handle deadlock
		lc[rid].wait(ul, [&]
			{
				// an exclusive lock is strict, we can only
				// acquire a lock if there is no contention

				return !vec.empty();
			});

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
	// TODO: handle deadlock
	lc[rid].wait(ul, [&]
		{
			// only acquire the lock if the only lock left
			// is a shared lock that is owned by this txn
			const auto txId = txn->GetTransactionId();
			const bool cond_success = ((vec.size() == 1))
				&& (vec[0].GetType() == LockType::LOCK_SHARED)
				&& (vec[0].GetTxn()->GetTransactionId() == txId);

			return cond_success;
		});

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

	std::lock_guard<std::mutex> lg(mtx);
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
	// if the vector is empty, then remove the key from the unordered_map
	if (vec.empty())
		lm.erase(rid);

	return true;
}

} // namespace cmudb
