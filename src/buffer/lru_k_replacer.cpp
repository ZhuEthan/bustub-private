//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    std::lock_guard<std::mutex> lock(latch_);
    size_t mn_timestamp = current_timestamp_ + 1;
    size_t mn_k = k_;
    int ans = -1;
    for (auto iter : node_store_) {
        auto node = iter.second;
        if (!node.is_evictable_) continue;
        if (node.history_.size() < k_) {
            if (mn_k > 0) {
                mn_k = 0;
                mn_timestamp = current_timestamp_ + 1;
            }
            if (node.history_.front() < mn_timestamp) {
               mn_timestamp = node.history_.front();
               ans = node.fid_;
            }
        } else if (node.history_.size() == mn_k) {
            if (node.history_.back() < mn_timestamp) {
                mn_timestamp = node.history_.back();
                ans = node.fid_;
            }
        }
    }

    if (ans == -1) return false;
    *frame_id = ans;
    --curr_size_;
    node_store_.erase(ans);
    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id >= frame_id_t(replacer_size_)) {
        throw Exception("[RecordAccess] frame id is invalid");
    }
    current_timestamp_++;
    if (node_store_.find(frame_id) == node_store_.end()) {
        node_store_[frame_id] = LRUKNode(k_, frame_id, current_timestamp_);
    } else {
        node_store_[frame_id].pushTimestamp(current_timestamp_);
    }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    std::lock_guard<std::mutex> lock(latch_);
    if (frame_id >= frame_id_t(replacer_size_)) throw Exception("[SetEvictable] frame id is invalid");
    if (node_store_.find(frame_id) != node_store_.end()) {
        if (node_store_[frame_id].is_evictable_ != set_evictable) {
            node_store_[frame_id].is_evictable_ = set_evictable;
            if (set_evictable)
                curr_size_++;
            else
                curr_size_--;
        }
    }
}

void LRUKReplacer::Remove(frame_id_t frame_id) {}

auto LRUKReplacer::Size() -> size_t { return curr_size_; }

}  // namespace bustub
