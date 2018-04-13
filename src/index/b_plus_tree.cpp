/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

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
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>
		(buffer_pool_manager_->FetchPage(page_id));

	// lookup doesn't trigger any writes
	const bool is_dirty = false;
	// for an internal node, we need to keep going down the tree
	// till we hit the leaf node
	while (pg->IsLeafPage() == false)
	{
		B_PLUS_TREE_INTERNAL_PAGE_TYPE* internal =
			reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(pg);
		ValueType val = internal->Lookup(key, comparator_);
		// before reassigning internal, we need to unpin the page
		buffer_pool_manager_->UnpinPage(page_id, is_dirty);
		// for an internal node, value is always of type page_id_t

		// convert to page_id_t
		page_id = internal->Convert(val);
		pg = reinterpret_cast<BPlusTreePage*>
			(buffer_pool_manager_->FetchPage(page_id));
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
		buffer_pool_manager_->UnpinPage(page_id, is_dirty);
		return false;
	}

	// key found, append to vector
	result.push_back(val);
	buffer_pool_manager_->UnpinPage(page_id, is_dirty);
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

	// save this and check later if the root page id changes
	const page_id_t root_page_id = root_page_id_;
	if (!InsertIntoLeaf(key, value, transaction))
		return false;

	// TODO: change later, just to fix compilation issues
	if (root_page_id == root_page_id_)
		return true;

	return false;
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
	B_PLUS_TREE_LEAF_PAGE_TYPE* const leaf =
		reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>
		(buffer_pool_manager_->NewPage(page_id));

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
                                    Transaction *transaction) {

	// non-empty tree, we need to find the leaf node to insert
	BPlusTreePage* pg = reinterpret_cast<BPlusTreePage*>
		(buffer_pool_manager_->FetchPage(root_page_id_));
	B_PLUS_TREE_INTERNAL_PAGE_TYPE* internal;
	B_PLUS_TREE_LEAF_PAGE_TYPE* leaf;
	// vector to unpin the internal pages at the end
	std::vector<BPlusTreePage*> vec;
	ValueType val;
	page_id_t child;

	while (pg->IsLeafPage() == false)
	{
		vec.push_back(pg);
		internal = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE*>(pg);
		val = internal->Lookup(key, comparator_);
		child = internal->Convert(val);
		pg = reinterpret_cast<BPlusTreePage*>
			(buffer_pool_manager_->FetchPage(child));
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
		InsertIntoParent(reinterpret_cast<BPlusTreePage*>(leaf),
			key, reinterpret_cast<BPlusTreePage*>(split_leaf), transaction);
	}

	for (auto elem : vec)
	{
		buffer_pool_manager_->UnpinPage(elem->GetPageId(), false);
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

	N* const split_node = reinterpret_cast<N*>
		(buffer_pool_manager_->NewPage(page_id));

	// the split node has the same parent as that of the node
	split_node->Init(page_id, node->GetParentPageId());
	node->MoveHalfTo(split_node, buffer_pool_manager_);
	// once the split is done, the leaf and the split_leaf's writes
	// are done, so we can unpin them.
	buffer_pool_manager_->UnpinPage(split_node->GetPageId(), true);
	buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
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
                                      Transaction *transaction) {}

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
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  return false;
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
    int index, Transaction *transaction) {
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
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
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
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  return INDEXITERATOR_TYPE();
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
