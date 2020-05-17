#pragma once

#include <functional>
#include <list>
#include "loggers/index_logger.h"

namespace terrier::storage::index {
#define INVALID_NODE_ID ((NodeID)0UL)

template <typename KeyType, typename ValueType, typename KeyComparator = std::less<KeyType>,
          typename KeyEqualityChecker = std::equal_to<KeyType>, typename KeyHashFunc = std::hash<KeyType>,
          typename ValueEqualityChecker = std::equal_to<ValueType>>
class BPlusTree {
 public:

  using KeyValuePair = std::pair<KeyType, ValueType>;
  using NodeID = uint64_t;

  bool Insert(const KeyType& key, const ValueType& value, bool unique_key = false){


    while(1){
      Context context{key};
      std::pair<int, bool> index_pair;

      // Check whether the key-value pair exists
      // Also if the key previously exists in the delta chain
      // then return the position of the node using next_key_p
      // if there is none then return nullptr
      const KeyValuePair *item_p = Traverse(&context, &value, &index_pair, unique_key);

      // If the key-value pair already exists then return false
      if (item_p != nullptr) {
        epoch_manager.LeaveEpoch(epoch_node_p);

        return false;
      }

      //获取最新的快照(MVCC LATEST SNAPSHOT)
      NodeSnapshot *snapshot_p = GetLatestNodeSnapshot(&context);
      //获得快照中key的节点
      const BaseNode *node_p = snapshot_p->node_p;
      NodeID node_id = snapshot_p->node_id;

      //分配内存
      const LeafInsertNode *insert_node_p =
          LeafInlineAllocateOfType(LeafInsertNode, node_p, key, value, node_p, index_pair);

      //CAS更新mapping Table 把快照的结点node_p替换为要插入的结点insert_node_p
      bool ret = InstallNodeToReplace(node_id, insert_node_p, node_p);
      if (ret) {
        INDEX_LOG_TRACE("Leaf Insert delta CAS succeed");

        // If install is a success then just break from the loop
        // and return
        break;
      }

      //destructor
      insert_node_p->~LeafInsertNode();

    }
  }


 private:
  //member
  std::atomic<NodeID> root_id;

 private:
  //inline
  inline bool InstallNodeToReplace(NodeID node_id, const BaseNode *node_p, const BaseNode *prev_p) {
    // Make sure node id is valid and does not exceed maximum
    TERRIER_ASSERT(node_id != INVALID_NODE_ID, "Node count exceeded maximum.");
    TERRIER_ASSERT(node_id < MAPPING_TABLE_SIZE, "Node count exceeded maximum.");

    //CAS更新
    return mapping_table[node_id].compare_exchange_strong(prev_p, node_p);
  }
  static inline NodeSnapshot *GetLatestNodeSnapshot(Context *context_p) {
    return &context_p->current_snapshot;
  }

 private:
  //PRIVATE UTILS FUNCTION
  //bwtree的遍历
  const KeyValuePair Traverse(
      Context *context,
      const ValueType *value,
      std::pair<int, bool> *indexPair,
      bool isUnique = false
      ){

    const KeyValuePair *foundPair = nullptr;

    retry_traverse:
      NodeId start_node_id = root_id.load();


    abort_traverse:
      // This is used to identify root node
      context_p->current_snapshot.node_id = INVALID_NODE_ID;

      context_p->abort_flag = false;

      goto retry_traverse;

      return nullptr;


  }



};


}  // namespace terrier::storage::index
