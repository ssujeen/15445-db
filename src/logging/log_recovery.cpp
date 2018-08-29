/**
 * log_recovery.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record)
{
	/*
	*-------------------------------------------------------------
	* | size | LSN | transID | prevLSN | LogType |
	*-------------------------------------------------------------
	*/
	const uint32_t size = *reinterpret_cast<const uint32_t*>(data);
	const lsn_t lsn = *reinterpret_cast<const lsn_t*>(data + 4);
	const txn_id_t txn_id = *reinterpret_cast<const txn_id_t*>(data + 8);
	const lsn_t prev_lsn = *reinterpret_cast<const lsn_t*>(data + 12);
	const LogRecordType type = *reinterpret_cast<const LogRecordType*>(data + 16);
	const int HEADER_SIZE = 20;

	if (size == 0)
		return false;
	log_record.size_ = size;
	log_record.lsn_ = lsn;
	log_record.txn_id_ = txn_id;
	log_record.prev_lsn_ = prev_lsn;
	log_record.log_record_type_ = type;

	if (type == LogRecordType::COMMIT || type == LogRecordType::ABORT
		|| type == LogRecordType::BEGIN)
	{
		if (type == LogRecordType::BEGIN)
			assert(prev_lsn == INVALID_LSN);
		if (type == LogRecordType::ABORT || type == LogRecordType::COMMIT)
			assert(prev_lsn != INVALID_LSN);
		// nothing more to be done here
		return true;
	}

	// INSERT/DELETE*
	if (type == LogRecordType::INSERT)
	{
		log_record.insert_rid_ = *reinterpret_cast<const RID*>(data + HEADER_SIZE);
		log_record.insert_tuple_.DeserializeFrom(data + HEADER_SIZE + sizeof(RID));
	}
	else if (type == LogRecordType::MARKDELETE || type == LogRecordType::APPLYDELETE
		|| type == LogRecordType::ROLLBACKDELETE)
	{
		log_record.delete_rid_ = *reinterpret_cast<const RID*>(data + HEADER_SIZE);
		log_record.delete_tuple_.DeserializeFrom(data + HEADER_SIZE + sizeof(RID));
	}
	else if (type == LogRecordType::UPDATE)
	{
		log_record.update_rid_ = *reinterpret_cast<const RID*>(data + HEADER_SIZE);
		// get old tuple size
		const uint32_t old_ts = *reinterpret_cast<const uint32_t*>(data + HEADER_SIZE + sizeof(uint32_t));
		log_record.old_tuple_.DeserializeFrom(data + HEADER_SIZE + sizeof(RID));
		log_record.new_tuple_.DeserializeFrom(data + HEADER_SIZE + sizeof(RID) + sizeof(uint32_t) + old_ts);
	}
	else if (type == LogRecordType::NEWPAGE)
	{
		log_record.prev_page_id_ = *reinterpret_cast<const page_id_t*>(data + HEADER_SIZE);
		log_record.page_id_ = *reinterpret_cast<const page_id_t*>(data + HEADER_SIZE + sizeof(page_id_t));
	}

	return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo()
{
	bool ret;
	int fileOffset = 0;
	int bufferOffset = 0;

	ret = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, fileOffset);
	while (ret)
	{
		LogRecord log_record;
		if (!DeserializeLogRecord((log_buffer_ + bufferOffset), log_record))
			break;
		if ((bufferOffset + log_record.GetSize()) > LOG_BUFFER_SIZE)
		{
			// this happens if the log record spans across 2 LOG_BUFFER_SIZE blocks
			bufferOffset = 0;
			ret = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, fileOffset);
			continue;
		}
		bufferOffset += log_record.GetSize();
		fileOffset += log_record.GetSize();

		// there is no checkpointing here, so if a transaction's log records
		// are present, then the BEGIN transaction must be the first record

		LogRecordType& type = log_record.GetLogRecordType();
		const txn_id_t txn_id = log_record.GetTxnId();
		const lsn_t lsn = log_record.GetLSN();

		if (type != LogRecordType::COMMIT)
		{
			active_txn_[txn_id] = lsn;
			// no need to update lsn_mapping
			// since lsn itself is fileoffset
		}
		else
		{
			// erase from the active_txn_
			// since this doesn't have to undone
			active_txn_.erase(txn_id);
		}

		// do the actual redo here..
		if (log_record.log_record_type_ == LogRecordType::INSERT)
		{
			// redo the insert
			const page_id_t page_id = log_record.insert_rid_.GetPageId();
			Page* page;

			page = buffer_pool_manager_->FetchPage(page_id);
			const lsn_t disk_lsn = page->GetLSN();
			if (log_record.lsn_ <= disk_lsn)
			{
				// the changes persist in the disk, we don't need to do anything else
				buffer_pool_manager_->UnpinPage(page_id, false);
				continue;
			}

			// the changes doesn't persist in the disk
			// no need to log the redo stuff because the assumption is
			// we don't crash during recovery
			assert(reinterpret_cast<TablePage*>(page)->InsertTuple(log_record.insert_tuple_, log_record.insert_rid_, nullptr,
				nullptr, nullptr));

			buffer_pool_manager_->UnpinPage(page_id, true);
		}
		else if (log_record.log_record_type_ == LogRecordType::UPDATE)
		{
			// redo the update if necessary
			const page_id_t page_id = log_record.update_rid_.GetPageId();
			Page* page;

			page = buffer_pool_manager_->FetchPage(page_id);
			const lsn_t disk_lsn = page->GetLSN();
			if (log_record.lsn_ <= disk_lsn)
			{
				// the changes persist in the disk, we don't need to do anything else
				buffer_pool_manager_->UnpinPage(page_id, false);
				continue;
			}

			assert (reinterpret_cast<TablePage*>(page)->UpdateTuple(log_record.new_tuple_, log_record.old_tuple_,
				log_record.update_rid_, nullptr, nullptr, nullptr));
			buffer_pool_manager_->UnpinPage(page_id, true);
		}
		else if (log_record.log_record_type_ == LogRecordType::MARKDELETE)
		{
			// redo the markdelete if necessary
			const page_id_t page_id = log_record.update_rid_.GetPageId();
			Page* page;

			page = buffer_pool_manager_->FetchPage(page_id);
			const lsn_t disk_lsn = page->GetLSN();
			if (log_record.lsn_ <= disk_lsn)
			{
				// the changes persist in the disk, we don't need to do anything else
				buffer_pool_manager_->UnpinPage(page_id, false);
				continue;
			}

			assert (reinterpret_cast<TablePage*>(page)->MarkDelete(log_record.delete_rid_, nullptr,
				nullptr, nullptr));
			buffer_pool_manager_->UnpinPage(page_id, true);
		}
		else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE)
		{
			// redo the applydelete if necessary
			const page_id_t page_id = log_record.update_rid_.GetPageId();
			Page* page;

			page = buffer_pool_manager_->FetchPage(page_id);
			const lsn_t disk_lsn = page->GetLSN();
			if (log_record.lsn_ <= disk_lsn)
			{
				// the changes persist in the disk, we don't need to do anything else
				buffer_pool_manager_->UnpinPage(page_id, false);
				continue;
			}

			reinterpret_cast<TablePage*>(page)->ApplyDelete(log_record.delete_rid_, nullptr,
				nullptr);
			buffer_pool_manager_->UnpinPage(page_id, true);
		}
		else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE)
		{
			// redo the update if necessary
			const page_id_t page_id = log_record.update_rid_.GetPageId();
			Page* page;

			page = buffer_pool_manager_->FetchPage(page_id);
			const lsn_t disk_lsn = page->GetLSN();
			if (log_record.lsn_ <= disk_lsn)
			{
				// the changes persist in the disk, we don't need to do anything else
				buffer_pool_manager_->UnpinPage(page_id, false);
				continue;
			}

			reinterpret_cast<TablePage*>(page)->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
			buffer_pool_manager_->UnpinPage(page_id, true);
		}
		else if (log_record.log_record_type_ == LogRecordType::NEWPAGE)
		{
			const page_id_t page_id = log_record.page_id_;
			const page_id_t prev_page_id = log_record.prev_page_id_;
			Page* page;
			if (!disk_manager_->CheckPageValid(page_id))
			{
				page_id_t new_page_id;
				// the new page never made it to disk
				page = buffer_pool_manager_->NewPage(new_page_id);
				reinterpret_cast<TablePage*>(page)->Init(new_page_id, PAGE_SIZE,
					prev_page_id, nullptr, nullptr);
				buffer_pool_manager_->UnpinPage(new_page_id, true);
			}
			// if this page made it to disk, then the pageLSN of the page
			// in disk is >= lsn of this log record
		}
	}
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {}

} // namespace cmudb
