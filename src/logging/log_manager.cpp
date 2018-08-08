/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {

void LogManager::FlushThread()
{
	// the flush thread waits for 3 conditions
	// 1. buffer becomes full
	// 2. timeout
	// 3. buffer pool manager evicts a page
	// whose PageLSN > persistent_lsn_

	std::unique_lock<std::mutex> lg(mtx_);

	while (1)
	{
		lg.lock();
		if (cv_.wait_for(lg, std::chrono::seconds(LOG_TIMEOUT), [&] { return flush_ == true;}))
		{
			// due to pred success or timeout (when pred is success)
			disk_manager_.WriteLog(flush_buffer_, sz_);
			flush_ = false;
		}
		else
		{
			// due to timeout + pred fail
			// we do the swap here.
			assert (flush_ == false);
			temp_ = log_buffer_;
			log_buffer_ = flush_buffer_;
			flush_buffer_ = temp_;
			disk_manager_.WriteLog(flush_buffer, bytesWritten_);
			bytesWritten_ = 0;
		}

		lg.unlock();
	}
}
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread()
{
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record)
{
	// ugh!! this whole thing is protected by a mutex, so atomic is
	// useless. Doing it so that I dont mess with private vars. I am not sure
	// how to implement this without locks cleanly.

	std::unique_lock<std::mutex> lg(latch_);

	lg.lock();

	// first check if we have enough space in the log_buffer
	const LogRecordType type = log_record.log_record_type_;
	uint32_t updated_sz;
	lsn_t t_lsn;


	// in case the threads make *so* much progress before
    // the flush thread can flush the buffer to disk, we need
    // to wait. This happens **rarely**, so sleep should be sufficient	

	while (((bytesWritten_ + log_record.size_) > LOG_BUFFER_SIZE)
		&& (flush_ == true))
	{
		lg.unlock();
		std::this_thread::sleep_for(std::chrono::seconds(LOG_TIMEOUT));
		lg.lock();
	}

	if ((bytesWritten_ + log_record.size_) > LOG_BUFFER_SIZE)
	{
		if (vec_.empty())
		{
			flush_ = true;
			// swap the log and flush buffer
			temp_ = log_buffer_;
			log_buffer_ = flush_buffer_;
			flush_buffer_ = temp_;

			// update the sz of log buffer to write
			sz_ = bytesWritten_;
			// reset so that other threads can make use of a different buffer
			bytesWritten_ = 0;
			// unlock the mutex before notifying
			lg.unlock();
			// notify the flush thread
			cv_.notify_one();

			// acquire the mutex again
			lg.lock();
		}
	}

	// this is a bit tricky, what if the the thread that swaps the
	// buffer gets starved and the buffer gets filled up, then 
	// it is possible for it to skip the
	// same check above and hence the we need this check
	while (((bytesWritten_ + log_record.size_) > LOG_BUFFER_SIZE)
		&& (flush_ == true))
	{
		lg.unlock();
		std::this_thread::sleep_for(std::chrono::seconds(LOG_TIMEOUT));
		lg.lock();
	}

	assert ((log_record.size_ + bytesWritten_) <= LOG_BUFFER_SIZE);

	uint32_t offset = bytesWritten_;
	// copy to the log buffer at the right offset
	memcpy((log_buffer_ + offset), &log_record, 20);
	offset += 20;
	updated_sz = log_record.size_;

	switch (type)
	{
	case LogRecordType::BEGIN:
	case LogRecordType::COMMIT:
	case LogRecordType::ABORT:
	case LogRecordType::MARKDELETE:
	case LogRecordType::APPLYDELETE:
	case LogRecordType::ROLLBACKDELETE:
		memcpy((log_buffer_ + offset), &log_record.delete_rid_, sizeof(RID));
		offset += sizeof(RID);
		log_record.delete_tuple_.SerializeTo(log_buffer_ + offset);
		break;
	case LogRecordType::INSERT:
		// copy the tuple rid first
		memcpy((log_buffer_ + offset), &log_record.insert_rid_, sizeof(RID));
		offset += sizeof(RID);
		log_record.insert_tuple_.SerializeTo(log_buffer_ + offset);
	    break;
	case LogRecordType::UPDATE:
		memcpy((log_buffer_ + offset), &log_record.update_rid_, sizeof(RID));
		offset += sizeof(RID);
		log_record.old_tuple_.SerializeTo(log_buffer_ + offset);
		offset += (sizeof(uint32_t) + log_record.old_tuple_.GetLength());
		log_record.new_tuple_.SerializeTo(log_buffer_ + offset);
		break;
	case LogRecordType::NEWPAGE:
		memcpy((log_buffer_ + offset), &log_record.prev_page_id_, sizeof(page_id_t));
		offset += sizeof(page_id_t);
		break;
	default:
		assert (false);
	}


	t_lsn = next_lsn_;
	next_lsn_ += updated_sz;
	bytesWritten_ += updated_sz;

	assert (bytesWritten_ <= LOG_BUFFER_SIZE);

	lg.unlock();
	return t_lsn;
}

} // namespace cmudb
