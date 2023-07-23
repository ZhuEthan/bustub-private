#include "storage/page/page_guard.h"
#include "buffer/buffer_pool_manager.h"

namespace bustub {

BasicPageGuard::BasicPageGuard(BasicPageGuard &&that) noexcept {
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    is_dropped_ = that.is_dropped_;
}

void BasicPageGuard::Drop() {
    bpm_->UnpinPage(PageId(), is_dirty_);
    is_dropped_ = true;
}

auto BasicPageGuard::operator=(BasicPageGuard &&that) noexcept -> BasicPageGuard & { 
    bpm_ = that.bpm_;
    page_ = that.page_;
    is_dirty_ = that.is_dirty_;
    is_dropped_ = that.is_dropped_;
    return *this; 
}

BasicPageGuard::~BasicPageGuard(){
    if (!is_dropped_) bpm_->UnpinPage(PageId(), is_dirty_);
};  // NOLINT

ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept = default;

auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
    guard_ = std::move(that.guard_);
    return *this; 
}

void ReadPageGuard::Drop() {
    guard_.Drop();
    guard_.page_->RUnlatch();

}

ReadPageGuard::~ReadPageGuard() {
    if (!guard_.is_dropped_) guard_.page_->RUnlatch();
}  // NOLINT

WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept = default;

auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
    guard_ = std::move(that.guard_);
    return *this; 
}

void WritePageGuard::Drop() {
    guard_.Drop();
    guard_.page_->WUnlatch();

}

WritePageGuard::~WritePageGuard() {
    if (!guard_.is_dropped_) guard_.page_->WUnlatch();
}  // NOLINT

}  // namespace bustub
