/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 */

#pragma once
#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>
#include <chrono>
#include <thread>

#include "disk/disk_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogManager {
public:
  LogManager(DiskManager *disk_manager)
      : next_lsn_(0), persistent_lsn_(INVALID_LSN), th_(),
        disk_manager_(disk_manager) {
    // TODO: you may intialize your own defined member variables here
	flush_ = false;
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }
  // spawn a separate thread to wake up periodically to flush
  void RunFlushThread();
  void StopFlushThread();
  void FlushThread();
  void wake_flush_thread();
  void add_promise(std::promise<void> &promise);
  void add_promise_lsn(std::promise<lsn_t> &promise);

  // append a log record into log buffer
  lsn_t AppendLogRecord(LogRecord &log_record);

  // get/set helper functions
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

private:

  lsn_t next_lsn_;
  lsn_t persistent_lsn_;
  uint32_t bytesWritten_;
  uint32_t sz_;
  // log buffer related
  char *log_buffer_;
  char *flush_buffer_;
  char *temp_;
  bool flush_;

  // thread
  std::thread th_;

  // vector of promise objects
  std::vector<std::promise<void>> vec_;

  // vector of promise lsn objects
  std::vector<std::promise<lsn_t>> pvec_;

  // latch to protect shared member variables
  std::mutex latch_;
  // flush thread
  std::thread *flush_thread_;
  // for notifying flush thread
  std::condition_variable cv_;
  // disk manager
  DiskManager *disk_manager_;
};

} // namespace cmudb
