/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>
#include <stdexcept>
#include <cstring>
#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id)
{
	const size_t header_size = 24; // 8 byte aligned
	// calculate size to store the mapping array
	const size_t sz = PAGE_SIZE - header_size;
	const size_t elems = (sz / sizeof(MappingType));

	SetPageType(IndexPageType::LEAF_PAGE);
	SetPageId(page_id);
	SetParentPageId(parent_id);
	SetMaxSize(elems);
	SetSize(0);
	next_page_id_ = INVALID_PAGE_ID; // think this is the horizontal pointer
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id)
{
	next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  return 0;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  KeyType key;

  if ((index < 0) || (index >= GetSize()))
	  throw std::invalid_argument("received invalid index");

  key = array[index].first;

  return key;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetKeyAt(int index, KeyType &key)
{
  if ((index < 0) || (index >= GetSize()))
	  throw std::invalid_argument("received invalid index");

  array[index].first = key;
}
/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  if ((index < 0) || (index >= GetSize()))
	  throw std::invalid_argument("received invalid index");
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
	// return value is an integer, looks like there has to be some other
	// function that returns true/false depending on whether the key is
	// unique or not. Assume here that the key is unique
	int low = 0;
	int high = GetSize() - 1;
	int mid = (low + ((high - low) / 2));

	// assume we can overflow which will cause a split subsequently
	assert (GetSize() < GetMaxSize());
	while (low <= high)
	{
		// TODO: handle exception
		KeyType k1 = KeyAt(mid);
		if (comparator(key, k1) < 0)
		{
			// key is less than k1, then need to insert
			// in the first half
			high = mid - 1;
		}
		else if (comparator(key, k1) > 0)
		{
			// key is greater than k1, then need to insert
			// in the second half
			low = mid + 1;
		}
		else
		{
			// keys are unique, throw an exception here
			throw std::invalid_argument("received a duplicate key");
		}

		mid = (low + ((high - low) / 2));
	}

	// insert idx is at low, we need to shift [low, GetSize()) to
	// [low + 1, GetSize()] and insert the element at low
	assert (low <= GetSize());
	const int eltsToCopy = (GetSize() - low) * sizeof(MappingType);
	if (eltsToCopy)
		memmove(&array[low + 1], &array[low], eltsToCopy);
	array[low].first = key;
	array[low].second = value;
	IncreaseSize(1);

	const int sz = GetSize();
	const int maxSz = GetMaxSize();

	// this will return 0 if we need to split
	return (maxSz - sz) * sizeof(MappingType);
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager)
{

	// needs to be atleast 2
	assert(GetSize() > 1);
	const int mid = GetSize() / 2;
	MappingType* const items = &array[mid];
	recipient->CopyHalfFrom(items, GetSize() - mid);

	SetSize(mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size)
{
	// ensure that we don't cause an overflow
	const int currElements = GetSize();
	// make sure when we copy stuff from another page due to a  split
	// the split page should have 0 elements
	assert (currElements == 0);
	assert (currElements + size < GetMaxSize());
	MappingType* src = &array[currElements];
	for (int i = 0; i < size; i++)
	{
		src[i] = items[i];
	}

	IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
	int low = 0;
	int high = GetSize() - 1;
	int mid = (low + ((high - low) / 2));

	while (low <= high)
	{
		KeyType k1 = KeyAt(mid);
		if (comparator(key, k1) < 0)
		{
			// key is less than k1, then need to insert
			// in the first half
			high = mid - 1;
		}
		else if (comparator(key, k1) > 0)
		{
			// key is greater than k1, then need to insert
			// in the second half
			low = mid + 1;
		}
		else
		{
			// key is equal to k1
			// change the value
			value = array[mid].second;
			return true;
		}

		mid = (low + ((high - low) / 2));
	}

	return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {

	// if the key doesn't exist throw an exception
	MappingType elt;

	assert (GetSize() > 0);
	int low = 0;
	int high = GetSize() - 1;
	int mid = (low + ((high - low) / 2));
	const int maxSz = GetMaxSize();

	while (low <= high)
	{
		// TODO: handle exception
		KeyType k1 = KeyAt(mid);
		if (comparator(key, k1) < 0)
		{
			// key is less than k1, then need to insert
			// in the first half
			high = mid - 1;
		}
		else if (comparator(key, k1) > 0)
		{
			// key is greater than k1, then need to insert
			// in the second half
			low = mid + 1;
		}
		else
		{
			elt = array[mid];
			break;
		}

		mid = (low + ((high - low) / 2));
	}

	if (low > high) // we didn't find the elt
	{
		return (maxSz - GetSize()) * sizeof(MappingType);
	}

	// delete idx is at mid we need to copy [mid+1, GetSize)
	// to [mid, GetSize() - 1)
	const int eltsToCopy = (GetSize() - (mid + 1)) * sizeof(MappingType);
	if (eltsToCopy)
		memmove(&array[mid], &array[mid + 1], eltsToCopy);
	IncreaseSize(-1);

	const int sz = GetSize();

	// decide to merge depending on this return value
	return (maxSz - sz) * sizeof(MappingType);
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *)
{
	const int elemCount = GetSize();

	assert (elemCount > 0);
	recipient->CopyAllFrom(array, elemCount);

	// this is important, if we are merging with the left
	// sibling, then we can update the recipient's next pageid with
	// the next page id of *this* leaf node.
	// we shouldn't try to merge with the right sibling, then we can't
	// update the next page id properly.

	const int next_page_id = GetNextPageId();
	recipient->SetNextPageId(next_page_id);
	// there is nothing here
	SetSize(0);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size)
{
	// make sure we don't overflow
	assert (GetSize() + size <= GetMaxSize());

	// ensure that we move only from the right sibling to the left
	// or else we would mess the order
	MappingType* const arr = &array[GetSize()];
	for (int idx = 0; idx < size; idx++)
	{
		arr[idx] = items[idx];
	}

	IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relevant key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager)
{
	// this should only be called when trying to move a k-v pair from
	// the right sibling to the left sibling
	assert(GetSize() > 0);
	const MappingType item = GetItem(0);

	recipient->CopyLastFrom(item);
	IncreaseSize(-1);

	// need to resize
	memmove(&array[0], &array[1], GetSize() * sizeof(MappingType));

	// define a new function in internal node to handle this
	// TODO: find the key item.first in the parent and replace it with
	// array[0].first
	// don't forget to unpin the page after the write
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item)
{
	const int sz = GetSize();
	assert (sz < GetMaxSize());

	array[sz] = item;
	IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
	// this should only happen from the left sibling to the right sibling
	// to preserve order
	const int sz = GetSize();
	assert (sz > 0);

	auto elem = GetItem(sz - 1);
	recipient->CopyFirstFrom(elem, parentIndex, buffer_pool_manager);
	IncreaseSize(-1);

	// TODO: update in the parent
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager)
{
	const int sz = GetSize();
	assert ((sz > 1) && (sz < GetMaxSize()));

	memmove(&array[1], &array[0], sz * sizeof(MappingType));
	array[0] = item;
	IncreaseSize(1);

	// TODO: update the parent index's key
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
