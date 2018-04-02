#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"

namespace cmudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size)
{
	globalDepth = 0;
	numBuckets = 1;
	bucketSize = size;

	buckets = new Bucket* [1];
	buckets[0] = new Bucket;
	buckets[0]->localDepth = 0;
	buckets[0]->freeCount = size;
	buckets[0]->values = new V[size];
	buckets[0]->keys = new K[size];
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
	std::hash<K> khash;

	const size_t hash = khash(key);
	size_t nHash = 0;
	const size_t bits = sizeof(hash) * 8;
	const size_t maxWidth = (bits - 1);
	const size_t mask = (1 << globalDepth) - 1;

	for (size_t i = 0; i < bits; i++)
	{
		size_t bit = (hash >> i) & 1;
		nHash |= (bit << (maxWidth - i));
	}
	return (nHash >> (bits - globalDepth)) & mask;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
	return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
	return buckets[bucket_id]->localDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
	return numBuckets;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

	mtx.lock();
	size_t idx = HashKey(key);
	Bucket* bucket = buckets[idx];
	const int idxMax = (bucketSize - bucket->freeCount);
	for (int i = 0; i < idxMax; i++)
	{
		if (bucket->keys[i] == key)
		{
			value = bucket->values[i];
			mtx.unlock();
			return true;
		}
	}

	mtx.unlock();
	return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  size_t idx = HashKey(key);

  mtx.lock();
  Bucket* bucket = buckets[idx];
  const int idMax = bucketSize - bucket->freeCount;

  for (int i = 0; i < idMax; i++)
  {
	  if (bucket->keys[i] == key)
	  {
		  bucket->keys[i] = bucket->keys[idMax - 1];
		  bucket->values[i] = bucket->values[idMax - 1];
		  bucket->freeCount++;
		  mtx.unlock();
		  return true;
	  }
  }

  mtx.unlock();
  return false;
}

/*
 * insert a key without a split 
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::InsertWithoutSplit(const K &key, const V &value)
{
	const size_t iidx = HashKey(key);
	Bucket* bucket;

	bucket = buckets[iidx];

	// free space will be available always
	assert(bucket->freeCount > 0);
	
	const int idx = bucketSize - bucket->freeCount;
	bucket->keys[idx] = key;
	bucket->values[idx] = value;
	bucket->freeCount--;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value)
{
	V val;

	// lock acquired and released inside the function
	if (Find(key, val))
	{
		const size_t idx = HashKey(key);
		Bucket* bucket;

		mtx.lock();
		bucket = buckets[idx];

		for (size_t i = 0; i < (bucketSize - bucket->freeCount); i++)
		{
			if (bucket->keys[i] == key)
			{
				bucket->values[i] = value;
				mtx.unlock();
				return;
			}
		}

		// this should never happen
		mtx.unlock();
		assert(false);
		return;
	}

	mtx.lock();
	while (true)
	{
		// entry doesnt exist in the hash table
		const size_t idx = HashKey(key);
		Bucket* bucket;

		// case 1 -- free space available
		bucket = buckets[idx];

		if (bucket->freeCount)
		{
			const size_t iidx = (bucketSize - bucket->freeCount);
			bucket->freeCount--;
			bucket->keys[iidx] = key;
			bucket->values[iidx] = value;
			mtx.unlock();
			return;
		}
		else
		{
			// case 2 -- free space not available
			// case 2a -- local depth = global depth
			// need to split
			if (bucket->localDepth == globalDepth)
			{
				Bucket** newBuckets;
				newBuckets = new Bucket* [numBuckets << 1];
				globalDepth++;

				// the split is local, other buckets are not affected	
				for (size_t i = 0; i < idx; i++)
				{
					const int idx0 = ((i << 1) | 0);
					const int idx1 = ((i << 1) | 1);
					newBuckets[idx0] = buckets[i];
					newBuckets[idx1] = buckets[i];
				}

				for (size_t i = idx + 1; i < numBuckets; i++)
				{
					const int idx0 = ((i << 1) | 0);
					const int idx1 = ((i << 1) | 1);
					newBuckets[idx0] = buckets[i];
					newBuckets[idx1] = buckets[i];
				}
				// for the bucket that is affected by the split
				// we need to create an additional bucket	
				const int idx0 = ((idx << 1) | 0);
				const int idx1 = ((idx << 1) | 1);
				
				newBuckets[idx0] = new Bucket;
				newBuckets[idx1] = new Bucket;
				Bucket* t;

				t = newBuckets[idx0];
				t->localDepth = globalDepth;
				t->freeCount = bucketSize;
				t->keys = new K[bucketSize];
				t->values = new V[bucketSize];

				t = newBuckets[idx1];
				t->localDepth = globalDepth;
				t->freeCount = bucketSize;
				t->keys = new K[bucketSize];
				t->values = new V[bucketSize];

				numBuckets = (numBuckets << 1);
				// reinsert the old key-value pairs -- this shouldn't cause a split
				t = buckets[idx];
				delete [] buckets;
				buckets = newBuckets;

				for (size_t i = 0; i < (bucketSize - t->freeCount); i++)
				{
					InsertWithoutSplit(t->keys[i], t->values[i]);
				}
				delete [] t->keys;
				delete [] t->values;
				delete t;
				// now insert the new key-value pair -- this might cause a split
			}
			else
			{
				assert(bucket->localDepth < globalDepth);
				const size_t mask = (1 << (globalDepth - bucket->localDepth)) - 1;
				const size_t startIndex = (idx & (~mask));
				const size_t endIndex = (idx | mask);

				Bucket* b1 = new Bucket;
				Bucket* b2 = new Bucket;
				
				b1->freeCount = bucketSize;
				Bucket* save = buckets[startIndex]; // index doesn't matter, everything points to the same bucket
				size_t mid = (endIndex + startIndex + 1) / 2;
				for (size_t i = startIndex; i < mid; i++)
					buckets[i] = b1;
				b1->localDepth = save->localDepth + 1;
				b1->freeCount = bucketSize;
				b1->keys = new K[bucketSize];
				b1->values = new V[bucketSize];

				for (size_t i = mid; i <= endIndex; i++)
					buckets[i] = b2;
				b2->localDepth = save->localDepth + 1;
				b2->freeCount = bucketSize;
				b2->keys = new K[bucketSize];
				b2->values = new V[bucketSize];

				// re-insert the keys into the newly created buckets, wont cause a split
				
				for (size_t i = 0; i < bucketSize - save->freeCount; i++)
				{
					InsertWithoutSplit(save->keys[i], save->values[i]);
				}
				delete [] save->keys;
				delete [] save->values;
				delete save;

				// now try to insert the new key, might cause a split
			}
		}
	}	
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, int>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
