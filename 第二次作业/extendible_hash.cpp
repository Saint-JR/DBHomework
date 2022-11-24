#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
using namespace std;

namespace scudb {

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) :  globalDepth(0),bucketSize(size),bucketNum(1) {
  buckets.push_back(make_shared<Bucket>(0));
}
template<typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash() {
  ExtendibleHash(64);
}

/*
 * helper function to calculate the hashing address of input key
 */

//获取page_id的哈希值
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) const{
  return hash<K>{}(key);
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */

//获取GlobalDepth
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const{
  lock_guard<mutex> lock(latch);
  return globalDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */

//获取localDepth
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  //lock_guard<mutex> lck2(latch);
  if (buckets[bucket_id]) {
    lock_guard<mutex> lck(buckets[bucket_id]->latch);
    if (buckets[bucket_id]->kmap.size() == 0) return -1;
    return buckets[bucket_id]->localDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */

//获得已经使用的桶的数量
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const{
  lock_guard<mutex> lock(latch);
  return bucketNum;
}

/*
 * lookup function to find value associate with input key
 */

//根据page_id找里面存放的页表
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {

  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  if (buckets[idx]->kmap.find(key) != buckets[idx]->kmap.end()) {
    value = buckets[idx]->kmap[key];
    return true;

  }
  return false;
}


//获取桶的id
template <typename K, typename V>
int ExtendibleHash<K, V>::getIdx(const K &key) const{
  lock_guard<mutex> lck(latch);
  return HashKey(key) & ((1 << globalDepth) - 1);
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */

//删除该数据页表
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  int idx = getIdx(key);
  lock_guard<mutex> lck(buckets[idx]->latch);
  shared_ptr<Bucket> cur = buckets[idx];
  if (cur->kmap.find(key) == cur->kmap.end()) {
    return false;
  }
  cur->kmap.erase(key);
  return true;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */

//插入页表
// template <typename K, typename V>
// void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
//   //获得该桶的位置
//   int idx = getIdx(key);
//   //获得桶的指针
//   shared_ptr<Bucket> cur = buckets[idx];
//   //循环，直到分解的桶当中有空闲位置
//   while (true) {
//     //锁住该桶，进行同步
//     lock_guard<mutex> lck(cur->latch);
//     //如果该桶当中有重复则直接覆盖，跳出循环；或者该桶当中有空闲位置，则直接赋值后跳出循环
//     if (cur->kmap.find(key) != cur->kmap.end() || cur->kmap.size() < bucketSize) {
//       cur->kmap[key] = value;
//       break;
//     }
//     //若不能够插入该桶，则分裂该桶，并且使得localDepth+1：localDepth可以理解为该类型的桶在所有桶空间内还有多少空余
//     //当localDepth=globalDepth时则该桶空闲位置已经用完，需要所有桶分裂
//     //获取localDepth掩码
//     int mask = (1 << (cur->localDepth));
//     cur->localDepth++;

//     {
//       lock_guard<mutex> lck2(latch);
//       //当localDepth=globalDepth时则该桶空闲位置已经用完，直接将所有桶全部分裂（由于是直接分裂所有桶，那么其余桶必然会有指针复制
//       //所以localDepth=globalDepth表示该桶的空闲复制区间已被用完）
//       if (cur->localDepth > globalDepth) {

//         size_t length = buckets.size();
//         //全部复制一份，globalDepth+1
//         for (size_t i = 0; i < length; i++) {
//           buckets.push_back(buckets[i]);
//         }
//         globalDepth++;

//       }

//       //创建一个新桶
//       bucketNum++;
//       auto newBuc = make_shared<Bucket>(cur->localDepth);

//       typename map<K, V>::iterator it;
//       //遍历搜寻，将桶内的元素分为两组，利用掩码确定该元素到底是在分裂前的桶还是在分裂后的桶。
//       for (it = cur->kmap.begin(); it != cur->kmap.end(); ) {
//         //复制到分裂后的桶，并且在当前桶删除
//         if (HashKey(it->first) & mask) {
//           newBuc->kmap[it->first] = it->second;
//           it = cur->kmap.erase(it);
//         } else it++;
//       }

//       //遍历搜寻，将所有桶内指针指向当前桶并且和掩码按位与为1的置为新桶指针（这些所有置为新桶指针的位置也即新桶的空闲位置。
//       //localDepth和globalDepth之间的差值即为该桶空闲位置的数量）
//       for (size_t i = 0; i < buckets.size(); i++) {
//         if (buckets[i] == cur && (i & mask))
//           buckets[i] = newBuc;
//       }
//     }

//     //进行下一轮测试，看看该键值对知否能放进新桶，由于新桶是以localDepth前一位来分裂元素的
//     //若localDepth前一位为0则在原来的桶，前一位为1则到新的桶，若前一位全为0（或1）则原来的桶（新的桶）还是满的
//     //但若该键值对放入了还是满的桶，那么还得再一次进行分裂，直到将该键值对放入桶内
//     idx = getIdx(key);
//     cur = buckets[idx];
//   }
// }


template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  int index=getIdx(key);
  shared_ptr<Bucket> current_ptr = buckets[index];
  for(;;index = getIdx(key),current_ptr = buckets[index];){
    lock_guard<mutex> lck(cur->latch);
    if(current_ptr->kmap.find(key)){
      current_ptr->kmap[key]=value;
      break;
    }else if(current_ptr->kmap.size()<bucketSize){
      current_ptr->kmap.insert({key,value});
      break;
    }
    lock_guard<mutex> lck2(latch);
    current_ptr->localDepth++;
    bucketNum++;
    //local大于于global
    if(current_ptr->localDepth>globalDepth){
        //全部复制一份，globalDepth+1
        for (size_t i = 0; i <  buckets.size(); i++) {
          buckets.push_back(buckets[i]);
        }
      globalDepth++;
    }
    shared_ptr<Bucket> new_bucket = make_shared<Bucket>(current_ptr->localDepth);
    for(size_t i = 0; i <  buckets.size(); i++){
      if (buckets[i] == current_ptr && (i & (1 << (current_ptr->localDepth)))
          buckets[i] = new_bucket;
    }
    for(auto it = current_ptr->kmap.begin(); it != current_ptr->kmap.end(); ){
      //local前一位为1则为新桶
      if(HashKey(it->first)&(1 << (current_ptr->localDepth))){
        new_bucket->kmap.insert({it->first,it->second});
        it = cur->kmap.erase(it);
      }else it++;
    }
  }



  
}



template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
