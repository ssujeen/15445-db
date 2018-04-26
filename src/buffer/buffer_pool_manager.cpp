#include "buffer/buffer_pool_manager.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 const std::string &db_file)
    : pool_size_(pool_size), disk_manager_{db_file} {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(100);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  FlushAllPages();
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an entry
 * for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id)
{

	if (page_id == INVALID_PAGE_ID)
		return nullptr;
	// basically we need the contents of the disk page at offset (page_id * PAGE_SIZE)

	Page* page;
	latch_.lock();
	// case 1: the natural thing to do is to check if we already have it, if so increase the
	// ref count (pin count) and return the page
	if (page_table_->Find(page_id, page))
	{
		page->IncrementPinCount();
		// if the pin count becomes one, then the LRU entry should be made invalid
		// LRU contains a page whose contents are *valid* but has no threads referencing it
		// so, when a thread references the page, it should no longer be in the LRU because
		// if it was in the LRU, then it could be evicted while a thread references it
		if (page->GetPinCount() == 1)
		{
			replacer_->Erase(page);
		}
		// only insert in the LRU when the pin count = 0
		latch_.unlock();
		return page;
	}

	// case 2: page_id doesn't exist in the hash table, which means we need to bring the page to
	// memory. And we need a container for that. we can get it from either the free list or from the LRU
	
	// case 2a: get the container from the free list
	if (free_list_->size())
	{
		// free pages available
		page = free_list_->front();
		free_list_->pop_front();
	}

	// case 2b : get an elt from the LRU
	// the design is that LRU only contains pages that are unpinned
	// if a page is pinned, then there is already a thread that holds a reference to it
	// and we can't really use that container for another page_id	
	else if (replacer_->Victim(page))
	{
		// we need to first check if it is dirty, if so we need to flush it to disk
		// before we can re-use the container
		if (page->IsDirty())
		{
			assert(page->GetPageId() != INVALID_PAGE_ID);
			disk_manager_.WritePage(page->GetPageId(), page->GetData());
			page->SetDirty(false);
			// remove the dirty page from the map
			auto it = dirty_pages_.find(page->GetPageId());
			assert(it != end(dirty_pages_));
			dirty_pages_.erase(it);
		}
		// once we remove an entry from the LRU, we shouldn't be able to reference
		// the page with the page_id from before, since we will use that container
		// to store the contents of a different page id
		page_table_->Remove(page->GetPageId());
	}
	else
	{
		latch_.unlock();
		return nullptr;
	}

	// common ops for 2a and 2b
	disk_manager_.ReadPage(page_id, page->GetData());
	page->SetPageId(page_id);
	page->IncrementPinCount();
	assert(page->GetPinCount() == 1);
	assert(page->IsDirty() == false);
	page_table_->Insert(page_id, page);

	latch_.unlock();
	return page;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to replacer
 * if pin_count<=0 before this call, return false.
 * is_dirty: set the dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {

	Page* page;
	latch_.lock();
	if (!page_table_->Find(page_id, page))
	{
		latch_.unlock();
		return false;
	}

	assert (page->GetPageId() == page_id);
	assert (page->GetPageId() != INVALID_PAGE_ID);
	if (page->GetPinCount() <= 0)
	{
		latch_.unlock();
		return false;
	}
	// if the transition is from dirty->not dirty, we need to preserve
	// the dirty bit
	if (page->IsDirty() == true && is_dirty == false)
		is_dirty = true;
	page->SetDirty(is_dirty);
	page->DecrementPinCount();

	if (page->GetPinCount() == 0)
		replacer_->Insert(page);
	// insert the page into the dirty pages map
	if (is_dirty)
		dirty_pages_[page_id] = page;

	latch_.unlock();
	return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
	Page* page;
	latch_.lock();
	if (!page_table_->Find(page_id, page))
	{
		latch_.unlock();
		return false;
	}
	assert (page->GetPageId() == page_id);
	assert (page->GetPageId() != INVALID_PAGE_ID);

	if (page->IsDirty())
	{
		disk_manager_.WritePage(page->GetPageId(), page->GetData());
		page->SetDirty(false);
		// remove the dirty page from the map
		auto it = dirty_pages_.find(page->GetPageId());
		assert(it != end(dirty_pages_));
		dirty_pages_.erase(it);
	}

	latch_.unlock();
	return true;
}

/*
 * Used to flush all dirty pages in the buffer pool manager
 */
void BufferPoolManager::FlushAllPages()
{
	latch_.lock();

	std::for_each(begin(dirty_pages_), end(dirty_pages_),
		[&](std::pair<page_id_t, Page*> elem)
		{
			Page* tpage;
			Page* const& page = elem.second;
			assert(page->IsDirty() == true);
			assert(page->GetPageId() != INVALID_PAGE_ID);
			assert(page_table_->Find(page->GetPageId(), tpage));
			disk_manager_.WritePage(page->GetPageId(),
				page->GetData());
			page->SetDirty(false);

		});
	dirty_pages_.clear();

	latch_.unlock();
}

/**
 * User should call this method for deleting a page. This routine will call disk
 * manager to deallocate the page.
 * First, if page is found within page table, buffer pool manager should be
 * reponsible for removing this entry out of page table, reseting page metadata
 * and adding back to free list. Second, call disk manager's DeallocatePage()
 * method to delete from disk file.
 * If the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) {
	Page* page;

	latch_.lock();
	if (page_table_->Find(page_id, page))
	{
		// if it is in the buffer pool, remove from it if the pin count is 0
		// reset the page and add it to the free list, the pin count = 0
		// implies that the page is also in the LRU replacer
		if (page->GetPinCount() == 0)
		{
			page_table_->Remove(page_id);
			replacer_->Erase(page);
			// remove from the dirty list as well
			auto it = dirty_pages_.find(page->GetPageId());
			assert(it != end(dirty_pages_));
			dirty_pages_.erase(it);
			disk_manager_.DeallocatePage(page_id);
			page->Reset();
			free_list_->push_front(page);
			latch_.unlock();
			return true;
		}
	}

	latch_.unlock();
	return false;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either from
 * free list or lru replacer(NOTE: always choose from free list first), update
 * new page's metadata, zero out memory and add corresponding entry into page
 * table.
 * return nullptr is all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) 
{
	Page* page;
	// select the container to hold the page_id's contents

	latch_.lock();
	// try the freelist first
	if (free_list_->size())
	{
		page = free_list_->front();
		free_list_->pop_front();
	}
	// try the LRU
	else if (replacer_->Victim(page))
	{
		// we need to first check if it is dirty, if so we need to flush it to disk
		// before we can re-use the container
		if (page->IsDirty())
		{
			assert(page->GetPageId() != INVALID_PAGE_ID);
			disk_manager_.WritePage(page->GetPageId(), page->GetData());
			page->SetDirty(false);
			// remove the dirty page from the map
			auto it = dirty_pages_.find(page->GetPageId());
			assert(it != end(dirty_pages_));
			dirty_pages_.erase(it);
		}
		// removing from the LRU, means that this container
		// is going to be used for some other page id
		// so remove it from the hash table as well since subsequent
		// lookups should fail
		page_table_->Remove(page->GetPageId());
	}
	else
	{
		// no space
		latch_.unlock();
		return nullptr;
	}

	// don't assign pageId without having a free container
	const page_id_t pageId = disk_manager_.AllocatePage();
	page_id = pageId;

	page->Reset();
	page->SetPageId(pageId);
	disk_manager_.ReadPage(page->GetPageId(), page->GetData());
	page->IncrementPinCount();
	assert (page->GetPinCount() == 1);
	page_table_->Insert(pageId, page);

	latch_.unlock();
	return page;
}
} // namespace cmudb
