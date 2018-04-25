/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* pg, BufferPoolManager* bpm);
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE* pg, KeyType key,
      KeyComparator comparator, BufferPoolManager* bpm);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++()
  {
	idx_++;
	if (idx_ >= it_->GetSize() && (it_->GetNextPageId() != INVALID_PAGE_ID))
	{
		auto page_ptr = buffer_pool_manager_->FetchPage(it_->GetNextPageId());
		assert (page_ptr != nullptr);
		auto next_it = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>
			(page_ptr->GetData());
		buffer_pool_manager_->UnpinPage(it_->GetPageId(), false);
		idx_ = 0;
		it_ = next_it;
	}

	return *this;
  }

private:
  // all we need is a bplus tree leaf page pointer
  B_PLUS_TREE_LEAF_PAGE_TYPE *it_;
  BufferPoolManager* buffer_pool_manager_;
  int idx_;
};

} // namespace cmudb
