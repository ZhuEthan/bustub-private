//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // TODO(students): remove this line after you have implemented the buffer pool manager

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  bool flag = false;
  frame_id_t a_frame_id;
  if (free_list_.size() > 0) {
    a_frame_id = free_list_.front();
    free_list_.pop_front();
    flag = true;
  } else {
    flag = replacer_->Evict(&a_frame_id);
    if (flag) {
      if (pages_[a_frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[a_frame_id].page_id_, pages_[a_frame_id].data_);
      }
      page_table_.erase(pages_[a_frame_id].page_id_);
    }
  }
  if (!flag) return nullptr;
  replacer_->RecordAccess(a_frame_id);
  replacer_->SetEvictable(a_frame_id, false);
  auto new_page_id = AllocatePage();
  page_table_[new_page_id] = a_frame_id;
  pages_[a_frame_id].ResetMemory();
  pages_[a_frame_id].page_id_ = new_page_id;
  pages_[a_frame_id].pin_count_ = 1;
  *page_id = new_page_id;

  return &pages_[a_frame_id];
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) != page_table_.end()) {
    replacer_->RecordAccess(page_table_[page_id]);
    replacer_->SetEvictable(page_table_[page_id], false);
    pages_[page_table_[page_id]].pin_count_++;
    return &pages_[page_table_[page_id]];
  }
  bool flag = false; 
  frame_id_t a_frame_id; 
  if (free_list_.size() > 0) {
    a_frame_id = free_list_.front();
    free_list_.pop_front();
    flag = true;
  } else {
    flag = replacer_->Evict(&a_frame_id); // reuse the same frame_id
    if (flag) {
      if (pages_[a_frame_id].is_dirty_) {
        disk_manager_->WritePage(pages_[a_frame_id].page_id_, pages_[a_frame_id].data_);
      }
      page_table_.erase(pages_[a_frame_id].page_id_);
    }
  }
  if (!flag) return nullptr;
  replacer_->RecordAccess(a_frame_id);
  replacer_->SetEvictable(a_frame_id, false);
  page_table_[page_id] = a_frame_id;
  disk_manager_->ReadPage(page_id, pages_[a_frame_id].data_);
  pages_[a_frame_id].page_id_ = page_id;
  pages_[a_frame_id].pin_count_ = 1;
  pages_[a_frame_id].is_dirty_ = false;
  return &pages_[a_frame_id];
}


auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) return false;
  auto frame_id = page_table_[page_id];
  pages_[frame_id].is_dirty_ = is_dirty;
  pages_[frame_id].pin_count_--;
  if (pages_[frame_id].pin_count_ == 0) {
    replacer_->SetEvictable(frame_id, true);
  }
  return true;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) return false;
  disk_manager_->WritePage(page_id, pages_[page_table_[page_id]].data_);
  pages_[page_table_[page_id]].is_dirty_ = false;
  return true;
}

void BufferPoolManager::FlushAllPages() {
  for (auto x : page_table_) FlushPage(x.first);
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  std::lock_guard<std::mutex> lock(latch_);
  if (page_table_.find(page_id) == page_table_.end()) return true;
  auto frame_id = page_table_[page_id];
  if (pages_[frame_id].pin_count_ > 0) return false;

  pages_[frame_id].page_id_ = INVALID_PAGE_ID;
  pages_[frame_id].ResetMemory();
  DeallocatePage(page_id);
  page_table_.erase(page_id);
  replacer_->Remove(frame_id);
  free_list_.push_back(frame_id);
  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, FetchPage(page_id)}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, NewPage(page_id)}; }

}  // namespace bustub
