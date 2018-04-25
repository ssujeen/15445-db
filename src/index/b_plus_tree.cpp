/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <stack>

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
	page_id = root_page_id_;
	LOG_DEBUG("fetching page_id : %d", page_id);
	Page* page_ptr = buffer_pool_manager_->FetchPage(page_id);
	assert (page_ptr != nullptr);
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>
		(page_ptr->GetData());

	// lookup doesn't trigger any writes
	const bool is_dirty = false;
	// for an internal node, we need to keep going down the tree
	// till we hit the leaf node
	while (pg->IsLeafPage() == false)
	{
		BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal =
			reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*>(pg);
		page_id_t page_id = internal->Lookup(key, comparator_);
		// before reassigning internal, we need to unpin the page
		LOG_DEBUG("unpinning page id %d", pg->GetPageId());
		buffer_pool_manager_->UnpinPage(pg->GetPageId(), is_dirty);
		// for an internal node, value is always of type page_id_t

		LOG_DEBUG("fetching page id %d", page_id);
		Page* page_ptr = buffer_pool_manager_->FetchPage(page_id);
		assert (page_ptr != nullptr);
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
		// unpin the page before returning
		buffer_pool_manager_->UnpinPage(leaf->GetPageId(), is_dirty);
		return false;
	}

	// key found, append to vector
	result.push_back(val);
	buffer_pool_manager_->UnpinPage(leaf->GetPageId(), is_dirty);
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
	// empty tree
	if (root_page_id_ == INVALID_PAGE_ID)
	{
		StartNewTree(key, value);
		// when creating a new tree, we need to insert into the
		// header page
		UpdateRootPageId(static_cast<int>(true));
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

	// non-empty tree, we need to find the leaf node to insert
	Page* page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	B_PLUS_TREE_LEAF_PAGE_TYPE* leaf;
	// vector to unpin the internal pages at the end
	std::vector<BPlusTreePage*> vec;
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

	// leaf is always dirty
	buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
	for (auto elem : vec)
	{
		// potentially, all the pages could be written during split
		buffer_pool_manager_->UnpinPage(elem->GetPageId(), true);
	}

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
	// handle empty tree
	if (root_page_id_ == INVALID_PAGE_ID)
		return;

	// non-empty tree, we need to find the leaf node to remove from
	Page* page_ptr = buffer_pool_manager_->FetchPage(root_page_id_);
	assert (page_ptr != nullptr);
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* internal;
	B_PLUS_TREE_LEAF_PAGE_TYPE* leaf;
	// vector to unpin the internal pages at the end
	std::stack<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>*> vec;
	page_id_t child;

	while (pg->IsLeafPage() == false)
	{
		internal = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(pg);
		vec.push(internal);
		child = internal->Lookup(key, comparator_);
		page_ptr = buffer_pool_manager_->FetchPage(child);
		assert (page_ptr != nullptr);
		pg = reinterpret_cast<BPlusTreePage*>(page_ptr->GetData());
	}

	while (vec.empty() == false)
	{
		auto elem = vec.top();
		vec.pop();
		buffer_pool_manager_->UnpinPage(elem->GetPageId(), false);
	}
	// got the leaf page
	leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(pg);
	const int sz = leaf->RemoveAndDeleteRecord(key, comparator_);
	const int thresh = (leaf->GetMaxSize() >> 1);

	// no parent and leaf's size is 0, which means empty tree
	if (leaf->GetParentPageId() == INVALID_PAGE_ID)
	{
		if (sz == 0)
		{
			assert (AdjustRoot(reinterpret_cast<BPlusTreePage*>(leaf)) == true);
			return;
		}
		buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
		return;
	}

	if (leaf->GetParentPageId() != INVALID_PAGE_ID
		&& (sz >= thresh))
	{
		// no need to coalesce/redistribute
		buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
		return;
	}

	// handle the coalesce/redistribute case
	// handle leaf node with valid parent specifically
	const bool delete_node = CoalesceOrRedistribute(leaf, transaction);
	// return true implies we need to delete the leaf
	const page_id_t leaf_id = leaf->GetPageId();
	// but before that let's get the parent
	page_id_t curr_id = leaf->GetParentPageId();
	if (delete_node)
	{
		buffer_pool_manager_->UnpinPage(leaf_id, false);
		buffer_pool_manager_->DeletePage(leaf_id);
	}
	else
	{
		buffer_pool_manager_->UnpinPage(leaf_id, true);
	}

	while (curr_id != INVALID_PAGE_ID)
	{
		auto page_ptr = buffer_pool_manager_->FetchPage(curr_id);
		assert (page_ptr != nullptr);
		auto curr = reinterpret_cast<BPlusTreeInternalPage<KeyType,
			page_id_t, KeyComparator>*>(page_ptr->GetData());
		const int thresh = curr->GetMaxSize() >> 1;
		// root
		const bool root_exit = (curr->GetParentPageId() == INVALID_PAGE_ID)
			&& (curr->GetSize() >= 2);
		const bool internal_exit = (curr->GetParentPageId() != INVALID_PAGE_ID)
			&& (curr->GetSize() >= thresh);

		if (root_exit || internal_exit)
		{
			buffer_pool_manager_->UnpinPage(curr_id, true);
			break;
		}
		const bool delete_node = CoalesceOrRedistribute(curr,
			transaction);
		// cache the next_id before delete the page to avoid
		// potential race
		const page_id_t next_id = curr->GetParentPageId();
		if (delete_node)
		{
			buffer_pool_manager_->UnpinPage(curr_id, false);
			buffer_pool_manager_->DeletePage(curr_id);
		}
		else
		{
			buffer_pool_manager_->UnpinPage(curr_id, true);
		}

		curr_id = next_id;
	}
}

/*
 * helper function to find the parent and the sibling
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::GetSiblingAndKeyIdx(N* const node,
	BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>* &parent,
	N* &sibling, int &keyIdx)
{

	const int32_t currIdx = parent->ValueIndex(node->GetPageId());
	assert (currIdx >= 0 && currIdx < node->GetSize());
	int32_t siblingIdx;
	if (currIdx == node->GetSize() - 1)
		siblingIdx = currIdx - 1;
	else
		siblingIdx = currIdx + 1;

	const page_id_t sibling_id = parent->ValueAt(siblingIdx);
	assert (sibling_id != INVALID_PAGE_ID);
	auto page_ptr = buffer_pool_manager_->FetchPage(sibling_id);
	assert (page_ptr != nullptr);
	sibling = reinterpret_cast<N*>(page_ptr->GetData());
	keyIdx = (siblingIdx > currIdx) ? siblingIdx : currIdx;
}

INDEX_TEMPLATE_ARGUMENTS
template<typename N>
void BPLUSTREE_TYPE::PutSibling(N* const sibling, bool is_dirty)
{
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
	auto page_ptr = buffer_pool_manager_->FetchPage(node->GetParentPageId());
	assert (page_ptr != nullptr);
	auto parent = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
	KeyComparator>*>(page_ptr->GetData());
	GetSiblingAndKeyIdx(node, parent, sibling, keyIdx);
	assert (keyIdx < parent->GetSize());
	assert(sibling != nullptr);
	const int eltCount = node->GetSize();
	const int space = sibling->GetMaxSize() - sibling->GetSize();
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
			PutSibling(sibling, false);
			buffer_pool_manager_->DeletePage(sibling_id);
		}
		else
		{
			// copied to sibling
			PutSibling(sibling, true);
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
				assert (node->GetSize() > 1);
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
		PutSibling(sibling, true);
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
	node->MoveAllTo(neighbor_node, -1, nullptr);

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
		return true;
	}
	// case 1
	else if (old_root_node->GetSize() == 1)
	{
		auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t,
			KeyComparator>*>(old_root_node);
		const page_id_t new_root_id = root->RemoveAndReturnOnlyChild();
		root_page_id_ = new_root_id;
		buffer_pool_manager_->UnpinPage(old_root_node->GetPageId(), false);
		buffer_pool_manager_->DeletePage(old_root_node->GetPageId());
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
std::string BPLUSTREE_TYPE::ToString(bool verbose) { return "Empty tree"; }

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
