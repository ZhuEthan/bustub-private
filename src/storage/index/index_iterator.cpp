/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *buffer_pool_manager, page_id_t cur, int index) {
    bpm_ = buffer_pool_manager;
    index_ = index;
    cur_ = cur;
    if (cur != -1) {
      auto guard = bpm_->FetchPageRead(cur);
      auto leaf = guard.As<LeafPage>();
      item_ = {leaf->KeyAt(index), leaf->ValueAt(index)};
      guard.SetDirty(false);
      guard.Drop();
    }
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() = default;  // NOLINT

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
    return cur_ == -1;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
    return item_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
    index_++;

    auto guard = bpm_->FetchPageRead(cur_);
    auto leaf = guard.As<LeafPage>();

    if (index_ >= leaf->GetSize()) {
      auto next_id = leaf->GetNextPageId();
      guard.SetDirty(false);
      guard.Drop();
      if (next_id != -1) {
        index_ = 0;
        cur_ = next_id;
        guard = bpm_->FetchPageRead(cur_);
        leaf = guard.As<LeafPage>();
        item_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
        guard.SetDirty(false);
        guard.Drop();
      } else {
        cur_ = -1;
        index_ = -1;
        item_ = {};
      }
    } else {
      item_ = {leaf->KeyAt(index_), leaf->ValueAt(index_)};
      guard.SetDirty(false);
      guard.Drop();
    }
    return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool {
    return bpm_ == itr.bpm_ && cur_ == itr.cur_ && index_ == itr.index_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool {
    return !(*this == itr); 
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
