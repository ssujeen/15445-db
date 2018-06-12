/**
 * page.h
 *
 * Wrapper around actual data page in main memory and also contains bookkeeping
 * information used by buffer pool manager like pin_count/dirty_flag/page_id.
 * Use page as a basic unit within the database system
 */

#pragma once

#include <cstring>
#include <iostream>

#include "common/config.h"
#include "common/rwmutex.h"

namespace cmudb {

class Page {
  friend class BufferPoolManager;

public:
  Page() { ResetMemory(); }
  ~Page(){};
  // get actual data page content
  inline char *GetData() { return data_; }
  // get page id
  inline page_id_t GetPageId() { return page_id_; }
  // get page pin count
  inline int GetPinCount() { return pin_count_; }
  inline void IncrementPinCount() {pin_count_++;}
  inline void DecrementPinCount() {pin_count_--;}
  inline bool IsDirty() {return is_dirty_;}
  inline void SetDirty(bool dirty) { is_dirty_ = dirty;}
  inline void SetPageId(page_id_t page_id) {page_id_ = page_id;}
  // method use to latch/unlatch page content
  inline void WUnlatch()
  {
	  std::cout << "WUnlatching page id :" << page_id_ << std::endl;
	  rwlatch_.WUnlock();
  }
  inline void WLatch()
  {
	  std::cout << "Wlatching page id : " << page_id_ << std::endl;
	  rwlatch_.WLock();
	  std::cout << "Wlatching done page id : " << page_id_ << std::endl;
  }
  inline void RUnlatch()
  {
	  std::cout << "RUnlatching page id: " << page_id_ << std::endl;
	  rwlatch_.RUnlock();

  }
  inline void RLatch()
  {
	  std::cout << "RLatching page id : " << page_id_ << std::endl;
	  rwlatch_.RLock();
  }
  inline void Reset()
  {
	  pin_count_ = 0;
	  is_dirty_ = false;
	  memset(data_, 0, PAGE_SIZE);
	  page_id_ = INVALID_PAGE_ID;
  }

  RWMutex* GetLock()
  {
	  return &rwlatch_;
  }

private:
  // method used by buffer pool manager
  inline void ResetMemory() { memset(data_, 0, PAGE_SIZE); }
  // members
  char data_[PAGE_SIZE]; // actual data
  page_id_t page_id_ = INVALID_PAGE_ID;
  int pin_count_ = 0;
  bool is_dirty_ = false;
  RWMutex rwlatch_;
};

} // namespace cmudb
