/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"

namespace scudb {

//初始化lru替换链表
template <typename T> LRUReplacer<T>::LRUReplacer() {
  head = make_shared<Node>();
  tail = make_shared<Node>();
  head->next = tail;
  tail->prev = head;
}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
//将该页调用到链表头，也即最后被移出lru队列的页
// template <typename T> void LRUReplacer<T>::Insert(const T &value) {
//   //锁住该lru
//   lock_guard<mutex> lck(latch);
//   //获得一个当前页指针
//   shared_ptr<Node> cur;

//   //如果找到该页表在lru队列当中，那么将其从队列当中删除并移动到队列头，若没有找到该页表则直接新建一个页表放入队列头
//   if (map.find(value) != map.end()) {
//     cur = map[value];
//     shared_ptr<Node> prev = cur->prev;
//     shared_ptr<Node> succ = cur->next;
//     prev->next = succ;
//     succ->prev = prev;
//   } else {
//     cur = make_shared<Node>(value);
//   }
//   shared_ptr<Node> fir = head->next;
//   cur->next = fir;
//   fir->prev = cur;
//   cur->prev = head;
//   head->next = cur;
//   map[value] = cur;
//   return;
// }

template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  //锁住该lru
  lock_guard<mutex> lck(latch);
  //获得一个当前页指针
  shared_ptr<Node> current_ptr;

  //如果找到该页表在lru队列当中，那么将其从队列当中删除并移动到队列头，若没有找到该页表则直接新建一个页表放入队列头
  if (map.find(value) != map.end()) {
    current_ptr = map[value];
    current_ptr->prev->next = current_ptr->next;
    current_ptr->next->prev = current_ptr->prev;
    current_ptr->next = head->next;
    head->next->prev = current_ptr;
    current_ptr->prev = head;
    head->next = current_ptr;
    map[value] = current_ptr;
  } else {
    current_ptr = make_shared<Node>(value);
    current_ptr->next = head->next;
    head->next->prev = current_ptr;
    current_ptr->prev = head;
    head->next = current_ptr;
    map.insert({value,current_ptr})
  }
  return;
}
/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */

//查看是否需要删除该页，使用的是value的引用，如若删除页表需要修改引用value的值
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  //锁住该页表
  lock_guard<mutex> lck(latch);
  if (map.empty()) {
    return false;
  }
  //查看该页表是否在队列尾部，如果在尾部那么从链表以及map当中删除该页表并且将该页表的value赋值给value引用。
  shared_ptr<Node> last = tail->prev;
  tail->prev = last->prev;
  last->prev->next = tail;
  value = last->val;
  map.erase(last->val);
  return true;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */

//擦除该页
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  //锁住该页表
  lock_guard<mutex> lck(latch);

  //查看是否能找到该页表，如果找到了那么从链表以及map当中删除
  if (map.find(value) != map.end()) {
    shared_ptr<Node> current_ptr = map[value];
    current_ptr->prev->next = current_ptr->next;
    current_ptr->next->prev = current_ptr->prev;
  }
  return map.erase(value);
}


//返回lru链表的大小
template <typename T> size_t LRUReplacer<T>::Size() {
  lock_guard<mutex> lck(latch);
  return map.size();
}









template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
