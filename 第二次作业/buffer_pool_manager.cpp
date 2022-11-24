#include "buffer/buffer_pool_manager.h"

namespace scudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */

//初始化缓冲池管理器
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
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
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 *
 * This function must mark the Page as pinned and remove its entry from LRUReplacer before it is returned to the caller.
 */

//进程使用该页表，进行内存页表调度
// Page *BufferPoolManager::FetchPage(page_id_t page_id) {
//   //锁住该缓冲池
//   lock_guard<mutex> lck(latch_);
//   Page *tar = nullptr;
//   //先在存放所有页表的哈希表中查找有没有该页表，如果有那么让pin值+1并且在lru队列中删除该页表，返回该页表的指针
//   if (page_table_->Find(page_id,tar)) { //1.1
//     tar->pin_count_++;
//     replacer_->Erase(tar);
//     return tar;
//   }
//   //1.2
//   //如果在储存所有页面的可扩展哈希表当中没有找到该页表，那么将该页表从外存当中调入内存。
//   //首先调用GetVictimPage()函数查看是否有可用空间，如果没有那么返回空指针
//   tar = GetVictimPage();
//   if (tar == nullptr) return tar;
//   //2
//   //如果有可用页面那么查看lru队列中调回的页面是否是脏页面，如果是那么就写回磁盘
//   if (tar->is_dirty_) {
//     disk_manager_->WritePage(tar->GetPageId(),tar->data_);
//   }
//   //3
//   //将调回磁盘的页表从哈希表中删除并且调入新的页表
//   page_table_->Remove(tar->GetPageId());
//   page_table_->Insert(page_id,tar);
//   //4
//   //放入该页表并且把pin值置为1（因为有进程使用）
//   disk_manager_->ReadPage(page_id,tar->data_);
//   tar->pin_count_ = 1;
//   tar->is_dirty_ = false;
//   tar->page_id_= page_id;

//   return tar;
// }

Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  //锁住该缓冲池
  lock_guard<mutex> lck(latch_);
  Page *fetch_page = nullptr;
  //先在存放所有页表的哈希表中查找有没有该页表，如果有那么让pin值+1并且在lru队列中删除该页表，返回该页表的指针
  if (page_table_->Find(page_id,fetch_page)) { //1.1
    fetch_page->pin_count_++;
    replacer_->Erase(fetch_page);
    return fetch_page;
  }else{
    //如果在储存所有页面的可扩展哈希表当中没有找到该页表，那么将该页表从外存当中调入内存。
    //首先调用GetVictimPage()函数查看是否有可用空间，如果没有那么返回空指针
    fetch_page = GetVictimPage();
    if (fetch_page == nullptr||fetch_page->page_id_ == INVALID_PAGE_ID) 
      return nullptr;
    //如果有可用页面那么查看lru队列中调回的页面是否是脏页面，如果是那么就写回磁盘
    if (fetch_page->is_dirty_) {
      disk_manager_->WritePage(fetch_page->GetPageId(),fetch_page->data_);
    }
    //将调回磁盘的页表从哈希表中删除并且调入新的页表
    page_table_->Remove(fetch_page->GetPageId());
    page_table_->Insert(page_id,fetch_page);
    //放入该页表并且把pin值置为1（因为有进程使用）
    disk_manager_->ReadPage(page_id,fetch_page->data_);
    fetch_page->pin_count_ = 1;
    fetch_page->is_dirty_ = false;
    fetch_page->page_id_= page_id;
    return fetch_page;
  }
  
}

//Page *BufferPoolManager::find

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */

//解除对该页表的控制
// bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
//   lock_guard<mutex> lck(latch_);
//   Page *tar = nullptr;
//   page_table_->Find(page_id,tar);
//   if (tar == nullptr) {
//     return false;
//   }
//   tar->is_dirty_ = is_dirty;
//   if (tar->GetPinCount() <= 0) {
//     return false;
//   }
//   ;
//   //如果页表pin值为0，那么将其加入lru链表当中
//   if (--tar->pin_count_ == 0) {
//     replacer_->Insert(tar);
//   }
//   return true;
// }

//解除对该页表的控制
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  lock_guard<mutex> lck(latch_);
  Page *unpin_page = nullptr;
  page_table_->Find(page_id,unpin_page);
  if (unpin_page == nullptr|| unpin_page->page_id_ == INVALID_PAGE_ID||unpin_page->GetPinCount() <= 0) {
    if(unpin_page->GetPinCount() <= 0)
      unpin_page->is_dirty_ = is_dirty;
    return false;
  }else{
    //如果页表pin值为0，那么将其加入lru链表当中
    unpin_page->pin_count_--;
    if (unpin_page->pin_count_ == 0) {
      replacer_->Insert(unpin_page);
    }
    return true;
  }
  
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */

//刷新页表
// bool BufferPoolManager::FlushPage(page_id_t page_id) {
//   lock_guard<mutex> lck(latch_);
//   Page *tar = nullptr;
//   page_table_->Find(page_id,tar);
//   //如果页表是脏页那么写回磁盘，并且将isdirty置为false
//   if (tar == nullptr || tar->page_id_ == INVALID_PAGE_ID) {
//     return false;
//   }
//   if (tar->is_dirty_) {
//     disk_manager_->WritePage(page_id,tar->GetData());
//     tar->is_dirty_ = false;
//   }

//   return true;
// }

//刷新页表
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  lock_guard<mutex> lck(latch_);
  Page *flush_page = nullptr;
  page_table_->Find(page_id,flush_page);
  //如果页表是脏页那么写回磁盘，并且将isdirty置为false
  if (flush_page == nullptr || flush_page->page_id_ == INVALID_PAGE_ID) {
    return false;
  }
  if (flush_page->is_dirty_) {
    disk_manager_->WritePage(page_id,flush_page->GetData());
    flush_page->is_dirty_ = false;
    return true;
  }else return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */

//删除一张页表
// bool BufferPoolManager::DeletePage(page_id_t page_id) {
//   lock_guard<mutex> lck(latch_);
//   Page *tar = nullptr;
//   page_table_->Find(page_id,tar);
//   if (tar != nullptr) {
//     //如果该页表被pin住了，那么返回false
//     if (tar->GetPinCount() > 0) {
//       return false;
//     }
//     //删除该页表，将其从lru链表和可扩展哈希表当中删除。并且重置了该指针的内存空间。
//     replacer_->Erase(tar);
//     page_table_->Remove(page_id);
//     tar->is_dirty_= false;
//     tar->ResetMemory();
//     free_list_->push_back(tar);
//   }
//   disk_manager_->DeallocatePage(page_id);
//   return true;
// }

//删除一张页表
bool BufferPoolManager::DeletePage(page_id_t page_id) {
  lock_guard<mutex> lck(latch_);
  Page *delete_page = nullptr;
  page_table_->Find(page_id,delete_page);
  if(delete_page->page_id_ == INVALID_PAGE_ID)
    return false;
  if (delete_page != nullptr) {
    //如果该页表被pin住了，那么返回false
    if (delete_page->GetPinCount() > 0) {
      return false;
    }
    //删除该页表，将其从lru链表和可扩展哈希表当中删除。并且重置了该指针的内存空间。
    replacer_->Erase(delete_page);
    page_table_->Remove(page_id);
    delete_page->is_dirty_= false;
    delete_page->ResetMemory();
    free_list_->push_back(delete_page);
  }
  disk_manager_->DeallocatePage(page_id);
  return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */

//新建一张页表
// Page *BufferPoolManager::NewPage(page_id_t &page_id) {
//   lock_guard<mutex> lck(latch_);
//   Page *tar = nullptr;
//   tar = GetVictimPage();
//   if (tar == nullptr) return tar;

//   //写入磁盘空间
//   page_id = disk_manager_->AllocatePage();
//   //2
//   //如果需要从lru当中调回的是脏页那么将其写回磁盘
//   if (tar->is_dirty_) {
//     disk_manager_->WritePage(tar->GetPageId(),tar->data_);
//   }
//   //3
//   //从内存当中删除该磁盘并且将新页表插入
//   page_table_->Remove(tar->GetPageId());
//   page_table_->Insert(page_id,tar);

//   //4
//   tar->page_id_ = page_id;
//   tar->ResetMemory();
//   tar->is_dirty_ = false;
//   tar->pin_count_ = 1;

//   return tar;
// }

//新建一张页表
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  lock_guard<mutex> lck(latch_);
  Page *new_page = nullptr;
  new_page = GetVictimPage();
  if (new_page == nullptr) 
    return nullptr;
  //写入磁盘空间
  page_id = disk_manager_->AllocatePage();
  //如果需要从lru当中调回的是脏页那么将其写回磁盘
  if (new_page->is_dirty_) {
    disk_manager_->WritePage(new_page->GetPageId(),new_page->data_);
  }
  //从内存当中删除该磁盘并且将新页表插入
  page_table_->Remove(new_page->GetPageId());
  page_table_->Insert(page_id,new_page);
  new_page->page_id_ = page_id;
  new_page->ResetMemory();
  new_page->is_dirty_ = false;
  new_page->pin_count_ = 1;

  return new_page;
}

//查看有没有空闲位置给到页表，查看freelist和lru链表空闲
// Page *BufferPoolManager::GetVictimPage() {
//   Page *tar = nullptr;

//   //先查看空闲链表freelist,如果没有空闲位置了那么去调用lru队列删除一个页表
//   if (free_list_->empty()) {
//     //如果lru队列的大小也是0，也即所有使用的页表都是被一些进程pin的，都不可调回磁盘当中，那么返回空指针
//     if (replacer_->Size() == 0) {
//       return nullptr;
//     }
//     //如果lru队列当中有可调用的页表，那么删除lru队列当中最后一张页表
//     replacer_->Victim(tar);
//   } else {
//     //空闲链表freelist有位置，那么直接减少freelist的一个空闲位置，插入一张页表，返回该指针
//     tar = free_list_->front();
//     free_list_->pop_front();
//     assert(tar->GetPageId() == INVALID_PAGE_ID);
//   }
//   assert(tar->GetPinCount() == 0);
//   return tar;
// }

Page *BufferPoolManager::GetVictimPage() {
  Page *victim = nullptr;

  //先查看空闲链表freelist,如果没有空闲位置了那么去调用lru队列删除一个页表
  if (free_list_->empty()) {
    //如果lru队列的大小也是0，也即所有使用的页表都是被一些进程pin的，都不可调回磁盘当中，那么返回空指针
    if (replacer_->Size() == 0) {
      return victim;
    }
    //如果lru队列当中有可调用的页表，那么删除lru队列当中最后一张页表
    replacer_->Victim(victim);
    return victim;
  } else {
    //空闲链表freelist有位置，那么直接减少freelist的一个空闲位置，插入一张页表，返回该指针
    victim = free_list_->front();
    free_list_->pop_front();
    return victim;
  }
}

} // namespace cmudb
