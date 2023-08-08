#include <sstream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const LeafPage *leaf_page, const KeyType &key) -> int {
  // std::cout << "binary" << leaf_page << std::endl;
  int l = 0;
  int r = leaf_page->GetSize() - 1;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    if (comparator_(leaf_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }
  if (r >= 0 && comparator_(leaf_page->KeyAt(r), key) == 1) {
    r = -1;
  }
  return r;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::BinaryFind(const InternalPage *internal_page, const KeyType &key) -> int {
  // if (internal_page == nullptr) return -1;

  // if (internal_page->GetSize() == 0) return 0;

  int l = 1;
  int r = internal_page->GetSize() - 1;
  // std::cout << "Binary: "
  //           << " key: " << key << " " << r << std::endl;
  while (l < r) {
    int mid = (l + r + 1) >> 1;
    // std::cout << "mid: " << mid << std::endl;
    if (comparator_(internal_page->KeyAt(mid), key) != 1) {
      l = mid;
    } else {
      r = mid - 1;
    }
  }
  if (r == -1 || comparator_(internal_page->KeyAt(r), key) == 1) {
    r = 0;
  }
  return r;
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return true; }




/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;

  if (header_page_id_ == INVALID_PAGE_ID) {
    return false;
  }

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return false;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();

  ctx.read_set_.emplace_back(std::move(root_page_guard));

  header_page_guard.SetDirty(false);
  header_page_guard.Drop();

  while (true) {
    if (root_page->IsLeafPage()) {
      auto *leaf = reinterpret_cast<const LeafPage *>(root_page);
      int index = BinaryFind(leaf, key);
      if (index < 0 || comparator_(leaf->KeyAt(index), key) != 0) {
        while (!ctx.read_set_.empty()) {
          ctx.read_set_.back().SetDirty(false);
          ctx.read_set_.back().Drop();
          ctx.read_set_.pop_back();
        }
        return false;
      }

      result->emplace_back(leaf->ValueAt(index));
      break;
    }

    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();

    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

    ctx.read_set_.emplace_back(std::move(root_page_guard));
  }

  while (!ctx.read_set_.empty()) {
    ctx.read_set_.back().SetDirty(false);
    ctx.read_set_.back().Drop();
    ctx.read_set_.pop_back();
  }

  return true;

}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitLeaf(LeafPage *leaf, const KeyType &key, const ValueType &value, page_id_t *new_id)
    -> KeyType {
  // 处理叶子分裂，new_id以指针的方式传回新page的id（右边的page），同时该函数返回上传的key

  bool put_left = false;  // key放在原来的节点，还是分裂出来的节点
  int mid = leaf->GetMinSize();  // 要将mid~最后的内容移动到新分裂的节点
  KeyType mid_key = leaf->KeyAt(mid);

  // 根据数量的奇偶性调整mid和put_left，保证右边数量比左边多,up_key为右边的第一个key
  if (comparator_(mid_key, key) == -1) {
    if (leaf->GetMaxSize() % 2 == 1) {
      mid++;
    }
  } else {
    if (leaf->GetMaxSize() % 2 == 0) {
      if (comparator_(leaf->KeyAt(mid - 1), key) == 1) {
        put_left = true;
        mid--;
      }
    } else {
      put_left = true;
    }
  }

  auto new_leaf_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_leaf = new_leaf_basic_guard.AsMut<LeafPage>();
  new_leaf_basic_guard.SetDirty(true);
  new_leaf_basic_guard.Drop();

  auto new_leaf_guard = bpm_->FetchPageWrite(*new_id);
  new_leaf = new_leaf_guard.AsMut<LeafPage>();
  new_leaf->Init(leaf_max_size_);

  int leaf_size = leaf->GetSize();
  for (int i = mid, j = 0; i < leaf_size; i++, j++) {
    // std::cout << "move" << std::endl;
    new_leaf->SetAt(j, leaf->KeyAt(i), leaf->ValueAt(i));
    new_leaf->IncreaseSize(1);
    leaf->IncreaseSize(-1);
  }

  auto put_in_leaf = leaf;
  if (!put_left) {
    put_in_leaf = new_leaf;
  }

    int idx = BinaryFind(put_in_leaf, key);

  for (int i = put_in_leaf->GetSize(); i > idx + 1; i--) {
    put_in_leaf->SetAt(i, put_in_leaf->KeyAt(i - 1), put_in_leaf->ValueAt(i - 1));
  }
  put_in_leaf->SetAt(idx + 1, key, value);
  put_in_leaf->IncreaseSize(1);

  new_leaf->SetNextPageId(leaf->GetNextPageId());
  leaf->SetNextPageId(*new_id);

  KeyType up_key = new_leaf->KeyAt(0);

  new_leaf_guard.SetDirty(true);
  new_leaf_guard.Drop();

  return up_key;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitInternal(InternalPage *internal, const KeyType &key, page_id_t *new_id,
                                   page_id_t new_child_id) -> KeyType {
  // 处理internal分裂，new_id以指针的方式传回新page的id（右边的page），同时该函数返回上传的key，这里的key参数为
  // 下面上传过来的key,new_child_id为下层新分裂出的节点的页面id，是key的右孩子（key是new_child_id对应页面的第一个上传过来的）

  bool put_left = false;
  int mid = internal->GetMinSize();
  KeyType mid_key = internal->KeyAt(mid);

  KeyType up_key{};
  page_id_t up_key_id = -1;  // up_key对应的孩子

  // 调整mid和up_key,保证右边数量比左边多,up_key为右边的第一个key
  if (comparator_(mid_key, key) == -1) {
    if (comparator_(key, internal->KeyAt(mid + 1)) == 1) {
      up_key = internal->KeyAt(mid + 1);
    } else {
      up_key = key;
    }
    mid++;
  } else {
    up_key = internal->KeyAt(mid);
    put_left = true;
  }

  up_key_id = internal->ValueAt(mid);


  // 将up_key删掉,如果up_key不是下面传过来的key的话（是的话就不用删，直接分成两半即可，然后把下面传上来的key再上传）
  if (comparator_(up_key, key) != 0) {
    for (int i = mid; i < internal->GetSize() - 1; i++) {
      internal->SetAt(i, internal->KeyAt(i + 1), internal->ValueAt(i + 1));
    }
    internal->IncreaseSize(-1);
  }

  auto new_internal_basic_guard = bpm_->NewPageGuarded(new_id);
  auto new_internal = new_internal_basic_guard.AsMut<InternalPage>();
  new_internal_basic_guard.SetDirty(true);
  new_internal_basic_guard.Drop();

}


INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::OptimalInsert(const KeyType &key, const ValueType &value, Transaction *txn) -> int {
  // 0表示要用悲观insert一次，1表示乐观insert成功，2表示重复key
  Context ctx;

  auto header_page_guard = bpm_->FetchPageRead(header_page_id_);
  auto header_page = header_page_guard.As<BPlusTreeHeaderPage>();

  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    header_page_guard.SetDirty(false);
    header_page_guard.Drop();
    return 0;
  }

  auto root_page_guard = bpm_->FetchPageRead(header_page->root_page_id_);
  auto root_page = root_page_guard.As<BPlusTreePage>();
  ctx.root_page_id_ = root_page_guard.PageId();

  ctx.read_set_.emplace_back(std::move(header_page_guard));

  while (true) {
    if (root_page->IsLeafPage()) {
      page_id_t leaf_id = root_page_guard.PageId();
      // 叶子节点放读锁
      root_page_guard.SetDirty(false);
      root_page_guard.Drop();

      // 叶子节点拿写锁
      auto leaf_guard = bpm_->FetchPageWrite(leaf_id);

      // 叶子节点父节点放读锁
      while (!ctx.read_set_.empty()) {
        ctx.read_set_.back().SetDirty(false);
        ctx.read_set_.back().Drop();
        ctx.read_set_.pop_back();
      }

      auto *leaf = leaf_guard.AsMut<LeafPage>();

      int index = BinaryFind(leaf, key);

      if (index >= 0 && comparator_(leaf->KeyAt(index), key) == 0) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        return 2;
      }

      if (leaf->GetSize() == leaf->GetMaxSize()) {
        leaf_guard.SetDirty(false);
        leaf_guard.Drop();
        return 0;
      }

      for (int i = leaf->GetSize(); i > index + 1; i--) {
        leaf->SetAt(i, leaf->KeyAt(i - 1), leaf->ValueAt(i - 1));
      }
      leaf->SetAt(index + 1, key, value);
      leaf->IncreaseSize(1);

      leaf_guard.SetDirty(true);
      leaf_guard.Drop();

      break;
    }

    while (!ctx.read_set_.empty()) {
      ctx.read_set_.back().SetDirty(false);
      ctx.read_set_.back().Drop();
      ctx.read_set_.pop_back();
    }

    auto *internal = reinterpret_cast<const InternalPage *>(root_page);

    int index = BinaryFind(internal, key);

    page_id_t child_id = internal->ValueAt(index);

    ctx.read_set_.emplace_back(std::move(root_page_guard));

    root_page_guard = bpm_->FetchPageRead(child_id);
    root_page = root_page_guard.As<BPlusTreePage>();

  }

  return 1;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return 0; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
