/**
 * lock_manager_test.cpp
 */

#include <thread>
#include <chrono>
#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {


/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    txn_mgr.Commit(&txn);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
  });

  t0.join();
  t1.join();
}

TEST(LockManagerTest, SharedExclusiveTestWithDeadlock)
{
	LockManager lock_mgr{false};
	TransactionManager txn_mgr{&lock_mgr};

	RID rid{0, 0};
	bool flag = false;

	std::thread t0([&]
		{
			Transaction txn(0);
			bool res = lock_mgr.LockExclusive(&txn, rid);
			EXPECT_EQ(res, true);
			// let the other thread run
			flag = true;
			// sleep for 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
	std::thread t1([&]
		{
			Transaction txn(1);
			while (flag == false)
				; // spin for the flag to become true
			// this will fail because the timestamp of txn1 > tstamp of t0
			bool res = lock_mgr.LockShared(&txn, rid);
			EXPECT_EQ(res, false);
		});
    t0.join();
    t1.join();
}

TEST(LockManagerTest, SharedExclusiveTestWithoutDeadlock)
{
	LockManager lock_mgr{false};
	TransactionManager txn_mgr{&lock_mgr};

	RID rid{0, 0};
	RID rid1{0, 1};
	bool flag = false;
	bool flag1 = false;

	std::thread t0([&]
		{
			Transaction txn(0);
			bool res = lock_mgr.LockExclusive(&txn, rid);
			EXPECT_EQ(res, true);
			EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
			flag1 = true;
			while (flag == false)
				; // wait for t1 to acquire the lock on rid1
			bool res1 = lock_mgr.LockExclusive(&txn, rid1);
			EXPECT_EQ(res1, true);
			EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
	std::thread t1([&]
		{
			Transaction txn(1);

			while (flag1 == false)
				; // wait for t0 to acquire the exclusive lock
			bool res = lock_mgr.LockShared(&txn, rid1);
			flag = true;
			EXPECT_EQ(res, true);
			// let the other thread run
			// sleep for 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
    t0.join();
    t1.join();
}

TEST(LockManagerTest, LockUpgradeTest)
{
	LockManager lock_mgr{false};
	TransactionManager txn_mgr{&lock_mgr};

	RID rid{0, 0};
	bool flag = false;
	bool flag1 = false;
	bool flag2 = false;

	std::thread t0([&]
		{
			Transaction txn(0);
			bool res = lock_mgr.LockShared(&txn, rid);
			flag2 = true; // t0 should acquire the shared lock first
						// to avoid the die scenario in deadlock prevention
			EXPECT_EQ(res, true);
			EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
			while (flag == false || flag1 == false)
				; // wait for t1 and t2 to acquire the shared lock on rid
			bool res1 = lock_mgr.LockUpgrade(&txn, rid);
			EXPECT_EQ(res1, true);
			EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
	std::thread t1([&]
		{
			Transaction txn(1);

			while (flag2 == false)
				; // wait for t0 to acquire the exclusive lock
			bool res = lock_mgr.LockShared(&txn, rid);
			flag = true;
			EXPECT_EQ(res, true);
			// let t0 run, t0 should be stuck waiting for t1 to release
			// the shared lock
			// sleep for 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
	std::thread t2([&]
		{
			Transaction txn(2);

			while (flag2 == false)
				; // wait for t0 to acquire the exclusive lock
			bool res = lock_mgr.LockShared(&txn, rid);
			flag1 = true;
			EXPECT_EQ(res, true);
			// let t0 run, t0 should be stuck waiting for t2 to release
			// the shared lock
			// sleep for 1 second
			std::this_thread::sleep_for(std::chrono::milliseconds(1000));
			txn_mgr.Commit(&txn);
			EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
		});
    t0.join();
    t1.join();
	t2.join();
}
} // namespace cmudb
