/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <stack>
#include <queue>
#include <thread>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"
namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const
{
	return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {

	// in the bplus tree we only know the root page id
	// the root could also be a leaf node if it is is the only leaf node
	// so it is better to cast it to BPlusTreePage* first before recasting
	// it specifically

	page_id_t page_id;

	// lock semantics
	// root_page_id_ is a shared variable
	// we need to protect access to root_page_id_ until the root page
	// can be explicitly locked
	// consider what would happen if we don't do it.
	// GetValue() gets the page_id as X and gets pre-empted before it could lock
	// the page_ptr corresponding to page_id X
	// Delete() will trigger the root page_id X to be deleted
	// GetValue() resumes and works with stale data
	// so if we think about it, we can only unlock the mutex, if we can release
	// the RWLock on the root page
	mtx_.lock();
	bool is_locked = true;	
	page_id = root_page_id_;
	LOG_DEBUG("fetching page_id : %d", page_id);
	Page* page_ptr = buffer_pool_manager_->FetchPage(page_id);
	assert (page_ptr != nullptr);
	// lookup acquires a read lock
	page_ptr->RLatch();
	Page* save_ptr = page_ptr;
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>
		(page_ptr->GetData());

	// for an internal node, we need to keep going down the tree
	// till we hit the leaf node
	while (pg->IsLeafPage() == false)
	{
		BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal =
			reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*>(pg);
		page_id_t page_id = internal->Lookup(key, comparator_);

		// for an internal node, value is always of type page_id_t
		LOG_DEBUG("fetching page id %d", page_id);
		page_ptr = buffer_pool_manager_->FetchPage(page_id);
		assert (page_ptr != nullptr);
		save_ptr->RUnlatch();
		buffer_pool_manager_->UnpinPage(save_ptr->GetPageId(), false);
		if (is_locked == true)
		{
			mtx_.unlock();
			is_locked = false;
		}
		page_ptr->RLatch();
		save_ptr = page_ptr;
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	}

	B_PLUS_TREE_LEAF_PAGE_TYPE* const leaf =
		reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);

	// find the key in the leaf is simple, just need to do a binary
	// search
	ValueType val;
	if (leaf->Lookup(key, val, comparator_) == false)
	{
		// key not found
		save_ptr->RUnlatch();
		buffer_pool_manager_->UnpinPage(save_ptr->GetPageId(), false);
		if (is_locked == true)
		{
			mtx_.unlock();
			is_locked = false;
		}

		return false;
	}

	// key found, append to vector
	result.push_back(val);
	save_ptr->RUnlatch();
	buffer_pool_manager_->UnpinPage(save_ptr->GetPageId(), false);
	if (is_locked == true)
	{
		mtx_.unlock();
		is_locked = false;
	}
	return true;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction)
{
	mtx_.lock();
	// empty tree
	if (root_page_id_ == INVALID_PAGE_ID)
	{
		StartNewTree(key, value);
		// when creating a new tree, we need to insert into the
		// header page
		UpdateRootPageId(static_cast<int>(true));
		mtx_.unlock();
		return true;
	}

	if (!InsertIntoLeaf(key, value, transaction))
		return false;

	return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value)
{
	// get a new page from the buffer pool manager
	// mark it as a leaf page and call the Init()
	page_id_t page_id = INVALID_PAGE_ID;
	Page* const page_ptr = buffer_pool_manager_->NewPage(page_id);
	assert (page_ptr != nullptr);
	B_PLUS_TREE_LEAF_PAGE_TYPE* const leaf =
		reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page_ptr->GetData());

	if (leaf == nullptr)
		throw std::runtime_error("unable to get a new page from buffer pool");

	leaf->Init(page_id, INVALID_PAGE_ID);
	leaf->Insert(key, value, comparator_);

	// now that we have written to the page, we need to mark it as dirty
	// and unpin the page
	const bool is_dirty = true;
	buffer_pool_manager_->UnpinPage(page_id, is_dirty);

	// mark this page_id as the root
	root_page_id_ = page_id;
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction)
{
	// we get here after acquiring the mutex
	bool is_locked = true;

	// non-empty tree, we need to find the leaf node to insert
	Page* page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	B_PLUS_TREE_LEAF_PAGE_TYPE* leaf;
	page_id_t child;

	// insert requires to acquire the write lock
	// lock the root
	page_ptr->WLatch();
	// also add to the txn's page set
	transaction->AddIntoPageSet(page_ptr);

	while (pg->IsLeafPage() == false)
	{
		internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		child = internal->Lookup(key, comparator_);
		page_ptr = buffer_pool_manager_->FetchPage(child);
		assert (page_ptr != nullptr);
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
		// lock the page before checking whether it is safe to insert or not
		page_ptr->WLatch();
		// now we need to make a decision. if this node has enough space
		// that it won't cause a split, then we can safely free the locks
		// we hold on this node's ancestors
		auto check_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		if (check_internal->SafeInsert() == true)
		{
			RemoveLatches(transaction, is_locked, false);
		}
		transaction->AddIntoPageSet(page_ptr);
	}

	// got the leaf page
	leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
	const int sz = leaf->Insert(key, value, comparator_);

	if (sz == 0)
	{
		// need to split
		B_PLUS_TREE_LEAF_PAGE_TYPE* const split_leaf = Split(leaf);
		// if we are splitting, then we need to insert the first key
		// in the split_leaf as the new key and the split_leaf's page_id
		// in the parent
		assert(split_leaf->GetSize() > 0);
		const KeyType key = split_leaf->KeyAt(0);
		// for a leaf node we need to change the next_page_id as well
		split_leaf->SetNextPageId(leaf->GetNextPageId());
		leaf->SetNextPageId(split_leaf->GetPageId());
		// also update the parent in the split leaf
		split_leaf->SetParentPageId(leaf->GetParentPageId());
		InsertIntoParent(reinterpret_cast<BPlusTreePage*>(leaf),
			key, reinterpret_cast<BPlusTreePage*>(split_leaf), transaction);
		buffer_pool_manager_->UnpinPage(split_leaf->GetPageId(), true);
	}

	// insert is done, remove remaining latches
	RemoveLatches(transaction, is_locked, true);
	return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node)
{
	page_id_t page_id;

	Page* const page_ptr = buffer_pool_manager_->NewPage(page_id);
	assert (page_ptr != nullptr);
	N* const split_node = reinterpret_cast<N*>(page_ptr->GetData());

	// the split node has the same parent as that of the node
	split_node->Init(page_id, node->GetParentPageId());
	node->MoveHalfTo(split_node, buffer_pool_manager_);

	return split_node;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction)
{
	assert (old_node->GetParentPageId() == new_node->GetParentPageId());
	const page_id_t parent_id = old_node->GetParentPageId();
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;

	// handle the case where we update the root
	if (parent_id == INVALID_PAGE_ID)
	{
		page_id_t root_id;
		Page* const page_ptr = buffer_pool_manager_->NewPage(root_id);
		assert (page_ptr != nullptr);
		internal = 
			reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*> (page_ptr->GetData());
		internal->Init(root_id, INVALID_PAGE_ID);

		internal->PopulateNewRoot(old_node->GetPageId(), key,
			new_node->GetPageId());

		// update the parent in the new node and the old node
		new_node->SetParentPageId(root_id);
		old_node->SetParentPageId(root_id);

		root_page_id_ = root_id;
		// update the root page_id in the header page
		UpdateRootPageId(0);
		// unpin the new root page
		buffer_pool_manager_->UnpinPage(root_id, true);
		return;
	}
	else
	{
		Page* const page_ptr = buffer_pool_manager_->FetchPage(parent_id);
		assert (page_ptr != nullptr);
		BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* const parent =
			reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*> (page_ptr->GetData());
		const int sz = parent->InsertNodeAfter(old_node->GetPageId(),
			key, new_node->GetPageId());

		if (sz == 0)
		{
			// need to split
			BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*
				const split_parent = Split(parent);
			// once we have split, we need to update the parent id of
			// all the pointers in the split node
			for (int i = 0; i < split_parent->GetSize(); i++)
			{
				const page_id_t child = split_parent->ValueAt(i);

				auto page_ptr = buffer_pool_manager_->FetchPage(child);
				auto page = reinterpret_cast<BPlusTreePage*>
					(page_ptr->GetData());
				page->SetParentPageId(split_parent->GetPageId());
				buffer_pool_manager_->UnpinPage(child, true);
			}
			// key at index 0 is dummy at the internal node, but it is
			// the key at the parent node
			InsertIntoParent(parent, split_parent->KeyAt(0), split_parent,
				transaction);
			// unpin
			buffer_pool_manager_->UnpinPage(split_parent->GetPageId(), true);
		}
		buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
	}
}

//helper
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveLatches(Transaction* transaction, bool &is_locked,
	bool writable)
{
	// unlock all the locks on the ancestors
	while (!transaction->GetPageSet()->empty())
	{
		auto page_pt = transaction->GetPageSet()->front();
		// unlock the page before unpinning
		page_pt->WUnlatch();
		buffer_pool_manager_->UnpinPage(page_pt->GetPageId(), writable);
		transaction->GetPageSet()->pop_front();
		// unlock the mtx
		if (is_locked == true)
		{
			is_locked = false;
			mtx_.unlock();
		}
	}
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction)
{
	bool is_locked = true;
	mtx_.lock();
	// handle empty tree
	if (root_page_id_ == INVALID_PAGE_ID)
	{
		mtx_.unlock();
		return;
	}

	// non-empty tree, we need to find the leaf node to remove from
	Page* page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	// lock the page
	page_ptr->WLatch();
	// add to the txn's locked page set
	transaction->AddIntoPageSet(page_ptr);

	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	B_PLUS_TREE_LEAF_PAGE_TYPE* leaf;
	page_id_t child;

	while (pg->IsLeafPage() == false)
	{
		internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		child = internal->Lookup(key, comparator_);
		page_ptr = buffer_pool_manager_->FetchPage(child);
		assert (page_ptr != nullptr);
		// never check for safe delete without first locking the page
		page_ptr->WLatch();
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());

		auto check_internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		if (check_internal->SafeDelete() == true)
		{
			RemoveLatches(transaction, is_locked, false);
		}
		transaction->AddIntoPageSet(page_ptr);
	}

	// got the leaf page
	leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
	const int avail_sz = leaf->RemoveAndDeleteRecord(key, comparator_);
	int thresh = (leaf->GetMaxSize() >> 1) * sizeof(MappingType);
	if (leaf->GetMaxSize() % 2)
		thresh += sizeof(MappingType);
	const int max_sz = leaf->GetMaxSize() * sizeof(MappingType);
	// if the available sz is the max, it means empty tree
	if (leaf->GetParentPageId() == INVALID_PAGE_ID)
	{
		// unlock the page first
		assert (is_locked == true);
		assert(transaction->GetPageSet()->size() == 1);

		if (avail_sz == max_sz)
		{
			// the page is going to be deleted, no need
			// to save the changes
			assert (is_locked == true);
			is_locked = false;
			RemoveLatches(transaction, is_locked, false);
			assert (AdjustRoot(reinterpret_cast<BPlusTreePage*>(leaf)) == true);
			mtx_.unlock();
			return;
		}
		RemoveLatches(transaction, is_locked, true);
		return;
	}

	if (leaf->GetParentPageId() != INVALID_PAGE_ID
		&& (avail_sz <= thresh))
	{
		RemoveLatches(transaction, is_locked, true);
		return;
	}

	// handle the coalesce/redistribute case
	// handle leaf node with valid parent specifically
	const bool delete_node = CoalesceOrRedistribute(leaf, transaction);
	// return true implies we need to delete the leaf
	const page_id_t leaf_id = leaf->GetPageId();
	assert (transaction->GetPageSet()->empty() != true);
	// we need to retrieve from the reverse order, since
	// we are moving from leaf to root
	auto page_pt = transaction->GetPageSet()->back();
	assert (page_pt->GetPageId() == leaf->GetPageId());
	transaction->GetPageSet()->pop_back();
	// unlock before unpinning the page
	page_pt->WUnlatch();
	if (delete_node)
	{
		buffer_pool_manager_->UnpinPage(leaf_id, false);
		buffer_pool_manager_->DeletePage(leaf_id);
	}
	else
	{
		buffer_pool_manager_->UnpinPage(leaf_id, true);
	}

	while (!transaction->GetPageSet()->empty())
	{
		auto page_ptr = transaction->GetPageSet()->back();
		assert (page_ptr != nullptr);

		auto curr = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(page_ptr->GetData());
		int thresh = curr->GetMaxSize() >> 1;
		if (curr->GetMaxSize() % 2)
			thresh += 1;
		const bool root_exit = (curr->GetParentPageId() == INVALID_PAGE_ID)
			&& (curr->GetSize() >= 2);
		const bool internal_exit = (curr->GetParentPageId() != INVALID_PAGE_ID)
			&& (curr->GetSize() >= thresh);

		if (root_exit || internal_exit)
		{
			RemoveLatches(transaction, is_locked, false);
			break;
		}
		const bool delete_node = CoalesceOrRedistribute(curr,
			transaction);

		const auto curr_id = page_ptr->GetPageId();
		// either way, we can safely unlock the page
		page_ptr->WUnlatch();
		// remove from the transaction page set
		transaction->GetPageSet()->pop_back();
		if (delete_node)
		{
			buffer_pool_manager_->UnpinPage(curr_id, false);
			buffer_pool_manager_->DeletePage(curr_id);
		}
		else
		{
			buffer_pool_manager_->UnpinPage(curr_id, true);
		}
	}
#if 0
	// handle for the possibility that Coalesce returns true ie
	// it deletes the parent, this can only happen when the parent
	// is the root and it has less than 2 pointers, in that case,
	// the page id belonging to the root will be in the txn page set
	// which needs to be removed
	while (!transaction->GetPageSet()->empty())
	{
		assert (transaction->GetPageSet()->size() == 1);
		auto page_pt = transaction->GetPageSet()->front();
		assert (page_pt->GetPinCount() == 0);
		transaction->GetPageSet()->pop_front();
	}
#endif
	if (is_locked)
		mtx_.unlock();
}

/*
 * helper function to find the parent and the sibling
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::GetSiblingAndKeyIdx(N* const node,
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* &parent,
	N* &sibling, int &keyIdx, Transaction* transaction)
{

	const int32_t currIdx = parent->ValueIndex(node->GetPageId());
	assert (currIdx >= 0 && currIdx < parent->GetSize());
	int32_t siblingIdx;
	if (currIdx == (parent->GetSize() - 1))
		siblingIdx = currIdx - 1;
	else
		siblingIdx = currIdx + 1;

	const page_id_t sibling_id = parent->ValueAt(siblingIdx);
	assert (sibling_id != INVALID_PAGE_ID);
	auto page_ptr = buffer_pool_manager_->FetchPage(sibling_id);
	assert (page_ptr != nullptr);
	// lock the sibling
	page_ptr->WLatch();
	transaction->AddIntoPageSet(page_ptr);
	sibling = reinterpret_cast<N*>(page_ptr->GetData());
	keyIdx = (siblingIdx > currIdx) ? siblingIdx : currIdx;
}

INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::PutSibling(N* const sibling, bool is_dirty,
	Transaction* transaction)
{
	auto page_ptr = transaction->GetPageSet()->back();
	assert (page_ptr->GetPageId() == sibling->GetPageId());
	transaction->GetPageSet()->pop_back();
	page_ptr->WUnlatch();
	buffer_pool_manager_->UnpinPage(sibling->GetPageId(), is_dirty);
}
/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction)
{
	N* sibling = nullptr;
	int keyIdx;
	// there is no need to lock the parent page, because it would be already
	// locked by the txn
	auto page_ptr = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	assert (page_ptr != nullptr);
	auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
	KeyComparator>*>(page_ptr->GetData());
	GetSiblingAndKeyIdx(node, parent, sibling, keyIdx, transaction);
	assert (keyIdx < parent->GetSize());
	assert(sibling != nullptr);
	const int eltCount = node->GetSize();
	// subtract by 1 because if the eltcount is GetMaxSize(), then we need
	// to split
	const int space = sibling->GetMaxSize() - sibling->GetSize() - 1;
	const bool coalesce = (eltCount <= space);
	bool delete_node = false;
	page_id_t parent_id = parent->GetPageId();
	const page_id_t page_id = parent->ValueAt(keyIdx);

	if (coalesce)
	{
		N* src;
		N* dst;
		if (page_id == sibling->GetPageId())
		{
			// right sibling, move from sibling to node
			// node is not deleted
			dst = node;
			src = sibling;
		}
		else
		{
			// left sibling, move from node to sibling
			dst = sibling;
			src = node;
			delete_node = true;
		}
		if (Coalesce(dst, src, parent, keyIdx, transaction))
		{
			// parent needs to be deleted
			assert (parent->GetParentPageId() == INVALID_PAGE_ID);
			const page_id_t del_page_id = parent->GetPageId();
			// delete from the txn's page set, so that we don't have
			// to deal with it later
			for (auto it = transaction->GetPageSet()->begin();
				it != transaction->GetPageSet()->end();)
			{
				auto page_pt = *it;
				if (page_pt->GetPageId() == del_page_id)
				{
					page_pt->WUnlatch();
					it = transaction->GetPageSet()->erase(it);
				}
				else
				{
					it++;
				}
			}
			buffer_pool_manager_->UnpinPage(parent->GetPageId(), false);
			AdjustRoot(reinterpret_cast<BPlusTreePage*>(parent));
			// update the parent in the dst and src
			dst->SetParentPageId(INVALID_PAGE_ID);
			src->SetParentPageId(INVALID_PAGE_ID);
			parent_id = INVALID_PAGE_ID;
		}

		if (!delete_node)
		{
			// copied to node
			// avoid potential race by caching sibling id
			const page_id_t sibling_id = sibling->GetPageId();
			PutSibling(sibling, false, transaction);
			buffer_pool_manager_->DeletePage(sibling_id);
		}
		else
		{
			// copied to sibling
			PutSibling(sibling, true, transaction);
		}
	}
	else
	{
		// redistribute
		int idx;
		if (page_id == sibling->GetPageId())
		{
			// right sibling, we need to borrow the first elt in
			// the sibling to the node
			idx = 0;
		}
		else
		{
			// left sibling, we need to borrow the last elt from the
			// sibling to the node
			idx = 1;
		}

		Redistribute(sibling, node, idx);

		if (node->IsLeafPage() == false)
		{
			// for internal node we have to do additional work
			KeyType key = parent->KeyAt(keyIdx);
			if (idx == 0)
			{
				// we have moved the first elt in the right sibling to the node
				// but the first elt has a dummy key. the actual key that
				// is the key at parent's keyIdx
				node->SetKeyAt(node->GetSize() - 1, key);
			}
			else
			{
				// we have moved the last elt from left sibling
				// we need to replace the dummy key in the node before
				// the redistribute happened, with the key at the keyIdx
				// in the parent
				node->SetKeyAt(1, key);
			}
		}
		if (idx == 0)
		{
			// the parent's key at keyIdx is no longer valid because
			// of the redistribute, so use the key at index 0 of the sibling
			// note that we use the key at index 0, because, before the
			// redistribute, the key would have been at index 1 which makes
			// it a valid key
			KeyType parentKey = sibling->KeyAt(0);
			parent->SetKeyAt(keyIdx, parentKey);
		}
		else
		{
			// the parent's key at keyIdx is no longer valid because
			// of the redistribute,so use the key at index 0 of the node
			// we use this key at index 0, because, we got this from the last
			// index in the left sibling which makes it a valid key
			KeyType parentKey = node->KeyAt(0);
			parent->SetKeyAt(keyIdx, parentKey);
		}
		// cleanup
		PutSibling(sibling, true, transaction);
	}

	// cleanup
	if (parent_id != INVALID_PAGE_ID)
		buffer_pool_manager_->UnpinPage(parent_id, true);
	return delete_node;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction)
{
	if (node->IsLeafPage() == false)
	{
		// for internal node, key at index 0 is dummy
		// we need to populate it with the key at index in the parent
		KeyType key = parent->KeyAt(index);
		node->SetKeyAt(0, key);

	}
	node->MoveAllTo(neighbor_node, -1, buffer_pool_manager_);

	parent->Remove(index);
	if (parent->GetParentPageId() == INVALID_PAGE_ID && parent->GetSize() == 1)
		return true;

    return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index)
{
	if (index == 0)
		neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
	else
		neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
	// case 2
	if (old_root_node->IsLeafPage())
	{
		assert (old_root_node->GetParentPageId() == INVALID_PAGE_ID);
		buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), true);
		buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
		root_page_id_ = INVALID_PAGE_ID;
		UpdateRootPageId(0);
		return true;
	}
	// case 1
	else if (old_root_node->GetSize() == 1)
	{
		auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*>(old_root_node);
		const page_id_t new_root_id = root->RemoveAndReturnOnlyChild();
		root_page_id_ = new_root_id;
		UpdateRootPageId(0);
		buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
		return true;
	}
    return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin()
{
	if (root_page_id_ == INVALID_PAGE_ID)
		return INDEXITERATOR_TYPE(nullptr, nullptr);

	auto page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	auto pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	std::vector<BPlusTreePage*> vec;
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	page_id_t child;

	while (pg->IsLeafPage() == false)
	{
		vec.push_back(pg);
		internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		child = internal->ValueAt(0);
		page_ptr = buffer_pool_manager_->FetchPage(child);
		assert (page_ptr != nullptr);
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	}

	// got the leaf page
	auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
	// unpin everything except the leaf
	for (auto elem : vec)
		buffer_pool_manager_->UnpinPage(elem->GetPageId(), false);

	return INDEXITERATOR_TYPE(leaf, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key)
{
	if (root_page_id_ == INVALID_PAGE_ID)
		return INDEXITERATOR_TYPE(nullptr, nullptr);

	auto page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	auto pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	std::vector<BPlusTreePage*> vec;
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	page_id_t child;

	while (pg->IsLeafPage() == false)
	{
		vec.push_back(pg);
		internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		child = internal->Lookup(key, comparator_);
		page_ptr = buffer_pool_manager_->FetchPage(child);
		assert (page_ptr != nullptr);
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	}

	// got the leaf page
	auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
	// unpin everything except the leaf
	for (auto elem : vec)
		buffer_pool_manager_->UnpinPage(elem->GetPageId(), false);

	return INDEXITERATOR_TYPE(leaf, key, comparator_, buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  return nullptr;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) 
{
	if (root_page_id_ == INVALID_PAGE_ID)
		return "";
	std::queue<std::pair<page_id_t, int>> queue;
	int current_level = 0;
	queue.push(std::make_pair(root_page_id_, current_level));
	std::string result;

	while (queue.empty() == false)
	{
		auto tup = queue.front();
		const page_id_t page_id = tup.first;
		const int lvl = tup.second;
		queue.pop();

		//
		if (lvl != current_level)
		{
			current_level = lvl;
			result += "\n";
		}
		auto page_ptr = buffer_pool_manager_->FetchPage(page_id);
		auto pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());

		if (pg->IsLeafPage() == false)
		{
			auto internal = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*>(pg);
			for (auto idx = 0; idx < internal->GetSize(); idx++)
			{
				queue.push(std::make_pair(internal->ValueAt(idx), current_level + 1));
			}

			result += internal->ToString(verbose);
		}
		else
		{
			auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
			result += leaf->ToString(verbose);
		}
		buffer_pool_manager_->UnpinPage(page_id, false);
	}

	return result;
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

	KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
