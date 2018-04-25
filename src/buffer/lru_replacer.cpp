/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {
	extHash = new ExtendibleHash<T, int>(1);
	clkIdx = 0;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {
	delete extHash;
}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {

	int idx;

	mtx.lock();
	// if the value is in the hash table, set the accessed bit
	if (extHash->Find(value, idx))
	{
		accessed[idx] = 1;
		// move the clkIdx if it points to the element whose
		// accessed bit is set
		if (clkIdx == idx)
		{
			clkIdx = (clkIdx + 1) % (vec.size());
		}
		mtx.unlock();
		return;
	}

	// add the elt to the vector and also to the hash table
	vec.push_back(value);
	idx = vec.size() - 1;
	accessed.push_back(0);

	assert((size_t)idx == (accessed.size() - 1));
	extHash->Insert(value, idx);
	mtx.unlock();
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
	int idx;
	mtx.lock();
	if (!vec.size())
	{
		mtx.unlock();
		return false;
	}

	// the <= is significant, if all elts are accesed, then it requires
	// size() + 1 iterations to choose an element as the victim
	size_t cnt;
	for (cnt = 0, idx = clkIdx; cnt <= vec.size(); cnt++)
	{
		if (accessed[idx] == 0)
		{
			value = vec[idx];
			extHash->Remove(vec[idx]);

			for (size_t i = idx + 1; i < vec.size(); i++)
			{
				extHash->Insert(vec[i], (i - 1));
			}
			vec.erase(vec.begin() + idx);
			accessed.erase(accessed.begin() + idx);
			// if the last element in the vector is the victim, then clkIdx
			// is not valid and needs to be pointed to the 0th index
			if (static_cast<size_t>(clkIdx) >= vec.size())
				clkIdx = 0;
			mtx.unlock();
			return true;
		}
		else
		{
			accessed[idx] = 0;
		}
		idx = (idx + 1) % vec.size();
	}

	mtx.unlock();
	// this should never happen
	assert(false);
	return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {

	int idx;

	mtx.lock();
	if (!vec.size())
	{
		mtx.unlock();
		return false;
	}
	if (!extHash->Find(value, idx))
	{
		mtx.unlock();
		return false;
	}

	assert (extHash->Remove(value));
	for (size_t i = idx + 1; i < vec.size(); i++)
	{
		extHash->Insert(vec[i], (i - 1));
	}
	vec.erase(vec.begin() + idx);
	accessed.erase(accessed.begin() + idx);

	// if the last element in the vector is the victim, then clkIdx
	// is not valid and needs to be pointed to the 0th index
	if (static_cast<size_t>(clkIdx) >= vec.size())
		clkIdx = 0;

	mtx.unlock();
	return true;
}

template <typename T> size_t LRUReplacer<T>::Size() {
	mtx.lock();
	size_t size = vec.size();
	mtx.unlock();

	return size;
}

template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
