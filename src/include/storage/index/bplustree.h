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
  using NodeID = uint64_t;
  using KeyValuePair = std::pair<KeyType, ValueType>;
  using KeyNodeIDPair = std::pair<KeyType, NodeID>;
  /*
   * enum class NodeType - Bw-Tree node type
   */
  enum class NodeType : short {
    // We separate leaf and inner into two different intervals
    // to make it possible for compiler to optimize

    InnerType = 0,

    // Only valid for inner
    InnerInsertType = 1,
    InnerDeleteType = 2,
    InnerSplitType = 3,
    InnerRemoveType = 4,
    InnerMergeType = 5,
    InnerAbortType = 6,  // Unconditional abort

    LeafStart = 7,

    // Data page type
    LeafType = 7,

    // Only valid for leaf
    LeafInsertType = 8,
    LeafSplitType = 9,
    LeafDeleteType = 10,
    LeafRemoveType = 11,
    LeafMergeType = 12,
  };

 private:
  class NodeMetaData {
   public:
    // For all nodes including base node and data node and SMO nodes,
    // the low key pointer always points to a KeyNodeIDPair structure
    // inside the base node, which is either the first element of the
    // node sep list (InnerNode), or a class member (LeafNode)
    const KeyNodeIDPair *low_key_p;

    // high key points to the KeyNodeIDPair inside the LeafNode and InnerNode
    // if there is neither SplitNode nor MergeNode. Otherwise it
    // points to the item inside split node or merge right sibling branch
    const KeyNodeIDPair *high_key_p;

    // The type of the node; this is forced to be represented as a short type
    NodeType type;

    // This is the depth of current delta chain
    // For merge delta node, the depth of the delta chain is the
    // sum of its two children
    short depth;

    // This counts the number of items alive inside the Node
    // when consolidating nodes, we use this piece of information
    // to reserve space for the new node
    int item_count;

    /*
     * Constructor
     */
    NodeMetaData(const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, NodeType p_type, int p_depth,
                 int p_item_count)
        : low_key_p{p_low_key_p},
          high_key_p{p_high_key_p},
          type{p_type},
          depth{static_cast<short>(p_depth)},
          item_count{p_item_count} {}
  };

  class BaseNode {
   private:
    NodeMetaData metadata;

   public:
    BaseNode(const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p, NodeType p_type, int p_depth,
             int p_item_count)
        : metadata{p_low_key_p, p_high_key_p, p_type, p_depth, p_item_count} {}

    /*
     * Type() - Return the type of node
     *
     * This method does not allow overridding
     */
    inline NodeType GetType() const { return metadata.type; }

    /*
     * GetNodeMetaData() - Returns a const reference to node metadata
     *
     * Please do not override this method
     */
    inline const NodeMetaData &GetNodeMetaData() const { return metadata; }

    /*
     * IsDeltaNode() - Return whether a node is delta node
     *
     * All nodes that are neither inner nor leaf type are of
     * delta node type
     */
    inline bool IsDeltaNode() const { return !(GetType() == NodeType::InnerType || GetType() == NodeType::LeafType); }

    /*
     * IsInnerNode() - Returns true if the node is an inner node
     *
     * This is useful if we want to collect all seps on an inner node
     * If the top of delta chain is an inner node then just do not collect
     * and use the node directly
     */
    inline bool IsInnerNode() const { return GetType() == NodeType::InnerType; }

    /*
     * IsRemoveNode() - Returns true if the node is of inner/leaf remove type
     *
     * This is used in JumpToLeftSibling() as an assertion
     */
    inline bool IsRemoveNode() const {
      return (GetType() == NodeType::InnerRemoveType) || (GetType() == NodeType::LeafRemoveType);
    }

    /*
     * IsOnLeafDeltaChain() - Return whether the node is part of
     *                        leaf delta chain
     *
     * This is true even for NodeType::LeafType
     *
     * NOTE: WHEN ADDING NEW NODE TYPES PLEASE UPDATE THIS LIST
     *
     * Note 2: Avoid calling this in multiple places. Currently we only
     * call this in TakeNodeSnapshot() or in the debugger
     *
     * This function makes use of the fact that leaf types occupy a
     * continuous region of NodeType numerical space, so that we could
     * the identity of leaf or Inner using only one comparison
     *
     */
    inline bool IsOnLeafDeltaChain() const { return GetType() >= NodeType::LeafStart; }

    /*
     * GetLowKey() - Returns the low key of the current base node
     *
     * NOTE: Since it is defined that for LeafNode the low key is undefined
     * and pointers should be set to nullptr, accessing the low key of
     * a leaf node would result in Segmentation Fault
     */
    inline const KeyType &GetLowKey() const { return metadata.low_key_p->first; }

    /*
     * GetHighKey() - Returns a reference to the high key of current node
     *
     * This function could be called for all node types including leaf nodes
     * and inner nodes.
     */
    inline const KeyType &GetHighKey() const { return metadata.high_key_p->first; }

    /*
     * GetHighKeyPair() - Returns the pointer to high key node id pair
     */
    inline const KeyNodeIDPair &GetHighKeyPair() const { return *metadata.high_key_p; }

    /*
     * GetLowKeyPair() - Returns the pointer to low key node id pair
     *
     * The return value is nullptr for LeafNode and its delta chain
     */
    inline const KeyNodeIDPair &GetLowKeyPair() const { return *metadata.low_key_p; }

    /*
     * GetNextNodeID() - Returns the next NodeID of the current node
     */
    inline NodeID GetNextNodeID() const { return metadata.high_key_p->second; }

    /*
     * GetLowKeyNodeID() - Returns the NodeID for low key
     *
     * NOTE: This function should not be called for leaf nodes
     * since the low key node ID for leaf node is not defined
     */
    inline NodeID GetLowKeyNodeID() const {
      TERRIER_ASSERT(!IsOnLeafDeltaChain(), "This should not be called on leaf nodes.");

      return metadata.low_key_p->second;
    }

    /*
     * GetDepth() - Returns the depth of the current node
     */
    inline int GetDepth() const { return metadata.depth; }

    /*
     * GetItemCount() - Returns the item count of the current node
     */
    inline int GetItemCount() const { return metadata.item_count; }

    /*
     * SetLowKeyPair() - Sets the low key pair of metadata
     */
    inline void SetLowKeyPair(const KeyNodeIDPair *p_low_key_p) { metadata.low_key_p = p_low_key_p; }

    /*
     * SetHighKeyPair() - Sets the high key pair of metdtata
     */
    inline void SetHighKeyPair(const KeyNodeIDPair *p_high_key_p) { metadata.high_key_p = p_high_key_p; }
  };

  /*
   * class DeltaNode - Common element in a delta node
   *
   * Common elements include depth of the node and pointer to
   * children node
   */
  class DeltaNode : public BaseNode {
   public:
    const BaseNode *child_node_p;

    /*
     * Constructor
     */
    DeltaNode(NodeType p_type, const BaseNode *p_child_node_p, const KeyNodeIDPair *p_low_key_p,
              const KeyNodeIDPair *p_high_key_p, int p_depth, int p_item_count)
        : BaseNode{p_type, p_low_key_p, p_high_key_p, p_depth, p_item_count}, child_node_p{p_child_node_p} {}
  };

  /*
   * class LeafDataNode - Holds LeafInsertNode and LeafDeleteNode's data
   *
   * This class is used in node consolidation to provide a uniform
   * interface for the log-structured merge process
   */
  class LeafDataNode : public DeltaNode {
   public:
    // This is the item being deleted or inserted
    KeyValuePair item;

    // This is the index of the node when inserting/deleting
    // the item into the base leaf node
    std::pair<int, bool> index_pair;

    LeafDataNode(const KeyValuePair &p_item, NodeType p_type, const BaseNode *p_child_node_p,
                 std::pair<int, bool> p_index_pair, const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p,
                 int p_depth, int p_item_count)
        : DeltaNode{p_type, p_child_node_p, p_low_key_p, p_high_key_p, p_depth, p_item_count},
          item{p_item},
          index_pair{p_index_pair} {}

    /*
     * GetIndexPair() - Returns the index pair for LeafDataNode
     *
     * Note that this function does not return reference which means
     * that there is no way to modify the index pair
     */
    std::pair<int, bool> GetIndexPair() const { return index_pair; }
  };

  /*
   * class LeafInsertNode - Insert record into a leaf node
   */
  class LeafInsertNode : public LeafDataNode {
   public:
    /*
     * Constructor
     */
    LeafInsertNode(const KeyType &p_insert_key, const ValueType &p_value, const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair)
        : LeafDataNode{std::make_pair(p_insert_key, p_value), NodeType::LeafInsertType, p_child_node_p, p_index_pair,
                       &p_child_node_p->GetLowKeyPair(), &p_child_node_p->GetHighKeyPair(),
                       p_child_node_p->GetDepth() + 1,
                       // For insert nodes, the item count is inheried from the child
                       // node + 1 since it inserts new item
                       p_child_node_p->GetItemCount() + 1} {}
  };

  /*
   * class LeafDeleteNode - Delete record from a leaf node
   *
   * In multi-value mode, it takes a value to identify which value
   * to delete. In single value mode, value is redundant but what we
   * could use for sanity check
   */
  class LeafDeleteNode : public LeafDataNode {
   public:
    /*
     * Constructor
     */
    LeafDeleteNode(const KeyType &p_delete_key, const ValueType &p_value, const BaseNode *p_child_node_p,
                   std::pair<int, bool> p_index_pair)
        : LeafDataNode{std::make_pair(p_delete_key, p_value), NodeType::LeafDeleteType, p_child_node_p, p_index_pair,
                       &p_child_node_p->GetLowKeyPair(), &p_child_node_p->GetHighKeyPair(),
                       p_child_node_p->GetDepth() + 1,
                       // For delete node it inherits item count from its child
                       // and - 1 from it since one element was deleted
                       p_child_node_p->GetItemCount() - 1} {}
  };

  /*
   * class LeafSplitNode - Split node for leaf
   *
   * It includes a separator key to direct search to a correct direction
   * and a physical pointer to find the current next node in delta chain
   */
  class LeafSplitNode : public DeltaNode {
   public:
    // Split key is the first element and the sibling NodeID
    // is the second element
    // This also specifies a new high key pair for the leaf
    // delta chain
    KeyNodeIDPair insert_item;

    /*
     * Constructor
     *
     * NOTE: The constructor requires that the physical pointer to the split
     * sibling being passed as an argument. It will not be stored inside the
     * split delta, but it will be used to compute the new item count for
     * the current node
     */
    LeafSplitNode(const KeyNodeIDPair &p_insert_item, const BaseNode *p_child_node_p, const BaseNode *p_split_node_p)
        : DeltaNode{NodeType::LeafSplitType, p_child_node_p, &p_child_node_p->GetLowKeyPair(),
                    // High key is redirected to the split item inside the node
                    &insert_item,
                    // NOTE: split node is SMO and does not introduce
                    // new piece of data
                    // So we set its depth the same as its child
                    p_child_node_p->GetDepth(),
                    // For split node it is a little bit tricky - we must
                    // know the item count of its sibling to decide how many
                    // items were removed by the split delta
                    p_child_node_p->GetItemCount() - p_split_node_p->GetItemCount()},
          insert_item{p_insert_item} {}
  };

  /*
   * class LeafRemoveNode - Remove all physical children and redirect
   *                        all access to its logical left sibling
   *
   * The removed ID field is not used in SMO protocol, but it helps
   * EpochManager to recycle NodeID correctly, since when a LeafRemoveNode
   * is being recycled, the removed NodeID is also recycled
   */
  class LeafRemoveNode : public DeltaNode {
   public:
    // This is used for EpochManager to recycle NodeID
    NodeID removed_id;

    /*
     * Constructor
     */
    LeafRemoveNode(NodeID p_removed_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::LeafRemoveType, p_child_node_p, &p_child_node_p->GetLowKeyPair(),
                    &p_child_node_p->GetHighKeyPair(),
                    // REMOVE node is an SMO and does not introduce data
                    p_child_node_p->GetDepth(), p_child_node_p->GetItemCount()},
          removed_id{p_removed_id} {}
  };

  /*
   * class LeafMergeNode - Merge two delta chian structure into one node
   *
   * This structure uses two physical pointers to indicate that the right
   * half has become part of the current node and there is no other way
   * to access it
   *
   * NOTE: Merge node also contains the NodeID of the node being removed
   * as the result of the merge operation. We keep it here to simplify
   * NodeID searching in the parent node
   */
  class LeafMergeNode : public DeltaNode {
   public:
    KeyNodeIDPair delete_item;

    // For merge nodes we use actual physical pointer
    // to indicate that the right half is already part
    // of the logical node
    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    LeafMergeNode(const KeyType &p_merge_key, const BaseNode *p_right_merge_p, NodeID p_deleted_node_id,
                  const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::LeafMergeType, p_child_node_p, &p_child_node_p->GetLowKeyPair(),
                    // The high key of the merge node is inherited
                    // from the right sibling
                    &p_right_merge_p->GetHighKeyPair(),
                    // std::max(p_child_node_p->metadata.depth,
                    //         p_right_merge_p->metadata.depth) + 1,
                    p_child_node_p->GetDepth() + p_right_merge_p->GetDepth(),
                    // For merge node the item count should be the
                    // sum of items inside both branches
                    p_child_node_p->GetItemCount() + p_right_merge_p->GetItemCount()},
          delete_item{p_merge_key, p_deleted_node_id},
          right_merge_p{p_right_merge_p} {}
  };

  ///////////////////////////////////////////////////////////////////
  // Leaf Delta Chain Node Type End
  ///////////////////////////////////////////////////////////////////

  ///////////////////////////////////////////////////////////////////
  // Inner Delta Chain Node Type
  ///////////////////////////////////////////////////////////////////

  /*
   * class InnerDataNode - Base class for InnerInsertNode and InnerDeleteNode
   *
   * We need this node since we want to sort pointers to such nodes using
   * stable sorting algorithm
   */
  class InnerDataNode : public DeltaNode {
   public:
    KeyNodeIDPair item;

    // This pointer points to the underlying InnerNode to indicate if
    // the search key >= key recorded in this delta node then the binary
    // search could start at this pointer's location; Similarly, if the
    // search key is smaller than this key then binary search could end before
    // this pointer
    const KeyNodeIDPair *location;

    InnerDataNode(const KeyNodeIDPair &p_item, NodeType p_type, const BaseNode *p_child_node_p,
                  const KeyNodeIDPair *p_location, const KeyNodeIDPair *p_low_key_p, const KeyNodeIDPair *p_high_key_p,
                  int p_depth, int p_item_count)
        : DeltaNode{p_type, p_child_node_p, p_low_key_p, p_high_key_p, p_depth, p_item_count},
          item{p_item},
          location{p_location} {}
  };

  /*
   * class InnerInsertNode - Insert node for inner nodes
   *
   * It has two keys in order to make decisions upon seeing this
   * node when traversing down the delta chain of an inner node
   * If the search key lies in the range between sep_key and
   * next_key then we know we should go to new_node_id
   */
  class InnerInsertNode : public InnerDataNode {
   public:
    // This is the next right after the item being inserted
    // This could be set to the +Inf high key
    // in that case next_item.second == INVALID_NODE_ID
    KeyNodeIDPair next_item;

    /*
     * Constructor
     */
    InnerInsertNode(const KeyNodeIDPair &p_insert_item, const KeyNodeIDPair &p_next_item,
                    const BaseNode *p_child_node_p, const KeyNodeIDPair *p_location)
        : InnerDataNode{p_insert_item,
                        NodeType::InnerInsertType,
                        p_child_node_p,
                        p_location,
                        &p_child_node_p->GetLowKeyPair(),
                        &p_child_node_p->GetHighKeyPair(),
                        p_child_node_p->GetDepth() + 1,
                        p_child_node_p->GetItemCount() + 1},
          next_item{p_next_item} {}
  };

  /*
   * class InnerDeleteNode - Delete node
   *
   * NOTE: There are three keys associated with this node, two of them
   * defining the new range after deleting this node, the remaining one
   * describing the key being deleted
   *
   * NOTE 2: We also store the InnerDeleteNode in BwTree to facilitate
   * tree destructor to avoid traversing NodeID that has already been
   * deleted and gatbage collected
   */
  class InnerDeleteNode : public InnerDataNode {
   public:
    // This holds the previous key-NodeID item in the inner node
    // if the NodeID matches the low key of the inner node (which
    // should be a constant and kept inside each node on the delta chain)
    // then do not need to compare to it since the search key must >= low key
    // But if the prev_item.second != low key node id then need to compare key
    KeyNodeIDPair prev_item;

    // This holds the next key-NodeID item in the inner node
    // If the NodeID inside next_item is INVALID_NODE_ID then we do not have
    // to conduct any comparison since we know it is the high key
    KeyNodeIDPair next_item;

    /*
     * Constructor
     *
     * NOTE: We need to provide three keys, two for defining a new
     * range, and one for removing the index term from base node
     *
     * NOTE 2: We also needs to provide the key being deleted, though
     * it does make sense for tree traversal. But when the tree destructor
     * runs it needs the deleted NodeID information in order to avoid
     * traversing to a node that has already been deleted and been recycled
     */
    InnerDeleteNode(const KeyNodeIDPair &p_delete_item, const KeyNodeIDPair &p_prev_item,
                    const KeyNodeIDPair &p_next_item, const BaseNode *p_child_node_p, const KeyNodeIDPair *p_location)
        : InnerDataNode{p_delete_item,
                        NodeType::InnerDeleteType,
                        p_child_node_p,
                        p_location,
                        &p_child_node_p->GetLowKeyPair(),
                        &p_child_node_p->GetHighKeyPair(),
                        p_child_node_p->GetDepth() + 1,
                        p_child_node_p->GetItemCount() - 1},
          prev_item{p_prev_item},
          next_item{p_next_item} {}
  };

  /*
   * class InnerSplitNode - Split inner nodes into two
   *
   * It has the same layout as leaf split node except for
   * the base class type variable. We make such distinguishment
   * to facilitate identifying current delta chain type
   */
  class InnerSplitNode : public DeltaNode {
   public:
    KeyNodeIDPair insert_item;

    /*
     * Constructor
     */
    InnerSplitNode(const KeyNodeIDPair &p_insert_item, const BaseNode *p_child_node_p, const BaseNode *p_split_node_p)
        : DeltaNode{NodeType::InnerSplitType, p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(),  // Low key does not change
                    &insert_item,                      // High key are defined by this
                                                       // For split node depth does not change since it does not
                                                       // introduce new data
                    p_child_node_p->GetDepth(),
                    // For split node we need the physical pointer to the
                    // split sibling to compute item count
                    p_child_node_p->GetItemCount() - p_split_node_p->GetItemCount()},
          insert_item{p_insert_item} {}
  };

  /*
   * class InnerRemoveNode
   */
  class InnerRemoveNode : public DeltaNode {
   public:
    // We also need this to recycle NodeID
    NodeID removed_id;

    /*
     * Constructor
     */
    InnerRemoveNode(NodeID p_removed_id, const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerRemoveType,        p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(), &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth(),       p_child_node_p->GetItemCount()},
          removed_id{p_removed_id} {}
  };

  /*
   * class InnerMergeNode - Merge delta for inner nodes
   */
  class InnerMergeNode : public DeltaNode {
   public:
    // This is exactly the item being deleted in the parent node
    KeyNodeIDPair delete_item;

    const BaseNode *right_merge_p;

    /*
     * Constructor
     */
    InnerMergeNode(const KeyType &p_merge_key, const BaseNode *p_right_merge_p, NodeID p_deleted_node_id,
                   const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerMergeType, p_child_node_p, &p_child_node_p->GetLowKeyPair(),
                    &p_right_merge_p->GetHighKeyPair(),
                    // Note: Since both children under merge node is considered
                    // as part of the same node, we use the larger one + 1
                    // as the depth of the merge node
                    // std::max(p_child_node_p->metadata.depth,
                    //         p_right_merge_p->metadata.depth) + 1,
                    p_child_node_p->GetDepth() + p_right_merge_p->GetDepth(),
                    // For merge node the item count is the sum of its two
                    // branches
                    p_child_node_p->GetItemCount() + p_right_merge_p->GetItemCount()},
          delete_item{p_merge_key, p_deleted_node_id},
          right_merge_p{p_right_merge_p} {}
  };

  /*
   * class InnerAbortNode - Same as LeafAbortNode
   */
  class InnerAbortNode : public DeltaNode {
   public:
    /*
     * Constructor
     */
    InnerAbortNode(const BaseNode *p_child_node_p)
        : DeltaNode{NodeType::InnerAbortType,         p_child_node_p,
                    &p_child_node_p->GetLowKeyPair(), &p_child_node_p->GetHighKeyPair(),
                    p_child_node_p->GetDepth(),       p_child_node_p->GetItemCount()} {}
  };

  //遍历到树某个节点时候看到的快照
  class NodeSnapshot {
   public:
    NodeID node_id;
    const BaseNode *node;

    NodeSnapshot(NodeID node_id, const BaseNode *node) : node_id{node_id}, node{node} {}

    NodeSnapshot() = default;

    inline bool IsLeaf() const { return node->IsOnLeafDeltaChain(); }
  };

  union DeltaNodeUnion {
    InnerInsertNode inner_insert_node;
    InnerDeleteNode inner_delete_node;
    InnerSplitNode inner_split_node;
    InnerMergeNode inner_merge_node;
    InnerRemoveNode inner_remove_node;
    InnerAbortNode inner_abort_node;

    LeafInsertNode leaf_insert_node;
    LeafDeleteNode leaf_delete_node;
    LeafSplitNode leaf_split_node;
    LeafMergeNode leaf_merge_node;
    LeafRemoveNode leaf_remove_node;
  };

  /***
   * 内存分配
   */

  class AllocationMeta {
   public:
    static constexpr size_t CHUNK_SIZE() { return sizeof(DeltaNodeUnion) * 8 + sizeof(AllocationMeta); }

   private:
    // points to the higher address end of the chunk
    std::atomic<char *> tail;
    // lower limit of the memory region
    char *const limit;
    // AllocationMeta form a linked list
    std::atomic<AllocationMeta *> next;

   public:
    AllocationMeta(char *pTail, char *pLimit) : tail{pTail}, limit{pLimit}, next{nullptr} {}

    void *TryAllocate(size_t size) {
      if (tail.load() < limit) {
        return nullptr;
      }
      // fetch_sub保证获取tail值然后减去size的操作是原子操作
      char *newPTail = tail.fetch_sub(size) - size;
      if (newPTail < limit) {
        return nullptr;
      }
      return newPTail;
    }

    /**
     * 在当前chunk之后分配一个新的chunk
     *
     * CAS保证线程安全
     *
     * @return
     */
    AllocationMeta *GrowChunk() {
      AllocationMeta *metaP = next.load();
      if (metaP != nullptr) {
        return metaP;
      }
      char *newChunk = new char[CHUNK_SIZE()];
      AllocationMeta *expected = nullptr;
      auto *newMetaBase = reinterpret_cast<AllocationMeta *>(newChunk);

      new (newMetaBase) AllocationMeta { newChunk + CHUNK_SIZE(), newChunk + sizeof(AllocationMeta) }  //

      bool CASResult = next.compare_exchange_strong(expected, newMetaBase);
      if (CASResult) {
        return newMetaBase;
      }
      newMetaBase->~AllocationMeta();
      delete[] newChunk;

      //  * Whether or not this has succeded, always return the pointer to the next
      //  * chunk such that the caller could retry on next chunk
      return expected;
    }

    /***
     * 类似于空闲链表分配的方法来管理free chunk
     */
    void *Allocate(size_t size) {
      AllocationMeta *currentP = this;
      while (1) {
        void *p = currentP->TryAllocate(size);
        if (p == nullptr) {
          //如果当前Chunk的空闲部分不允许分配size大小的内存，则分配一个新的chunk
          currentP = currentP->GrowChunk();
        } else {
          return p;
        }
      }
      return nullptr;
    }

    /***
     * delete掉所有的Chunk
     */
    void Destroy(){
      AllocationMeta* currentP = this;
      while(currentP!=nullptr){
        AllocationMeta* nextP = currentP->next.load();

        currentP->~AllocationMeta();
        delete[] reinterpret_cast<char*>(currentP);

        currentP = nextP;
      }
    }
  };

  /*
   * class ElasticNode - The base class for elastic node types, i.e. InnerNode
   *                     and LeafNode
   *
   * Since for InnerNode and LeafNode, the number of elements is not a compile
   * time known constant. However, for efficient tree traversal we must inline
   * all elements to reduce cache misses with workload that's less predictable
   */
  template <typename ElementType>
  class ElasticNode : public BaseNode {
   private:
    // These two are the low key and high key of the node respectively
    // since we could not add it in the inherited class (will clash with
    // the array which is invisible to the compiler) so they must be added here
    KeyNodeIDPair low_key;
    KeyNodeIDPair high_key;

    // This is the end of the elastic array
    // We explicitly store it here to avoid calculating the end of the array
    // everytime
    ElementType *end;

    // This is the starting point
    ElementType start[0];

   public:
    /*
     * Constructor
     *
     * Note that this constructor uses the low key and high key stored as
     * members to initialize the NodeMetadata object in class BaseNode
     */
    ElasticNode(NodeType p_type, int p_depth, int p_item_count, const KeyNodeIDPair &p_low_key,
                const KeyNodeIDPair &p_high_key)
        : BaseNode{p_type, &low_key, &high_key, p_depth, p_item_count},
          low_key{p_low_key},
          high_key{p_high_key},
          end{start} {}

    /*
     * Copy() - Copy constructs another instance
     */
    static ElasticNode *Copy(const ElasticNode &other) {
      ElasticNode *node_p = ElasticNode::Get(other.GetItemCount(), other.GetType(), other.GetDepth(),
                                             other.GetItemCount(), other.GetLowKeyPair(), other.GetHighKeyPair());

      node_p->PushBack(other.Begin(), other.End());

      return node_p;
    }

    /*
     * Destructor
     *
     * All element types are destroyed inside the destruction function. D'tor
     * is called by Destroy(), and therefore should not be called directly
     * by external functions.
     *
     * Note that this is not called by Destroy() and instead it should be
     * called by an external function that destroies a delta chain, since in one
     * instance of thie class there might be multiple nodes of different types
     * so destroying should be dont individually with each type.
     */
    ~ElasticNode() {
      // Use two iterators to iterate through all existing elements
      for (ElementType *element_p = Begin(); element_p != End(); element_p++) {
        // Manually calls destructor when the node is destroyed
        element_p->~ElementType();
      }
    }

    /*
     * Destroy() - Frees the memory by calling AllocationMeta::Destroy()
     *
     * Note that function does not call destructor, and instead the destructor
     * should be called first before this function is called
     *
     * Note that this function does not call destructor for the class since
     * it holds multiple instances of tree nodes, we should call destructor
     * for each individual type outside of this class, and only frees memory
     * when Destroy() is called.
     */
    void Destroy() const {
      // This finds the allocation header for this base node, and then
      // traverses the linked list
      ElasticNode::GetAllocationHeader(this)->Destroy();
    }

    /*
     * Begin() - Returns a begin iterator to its internal array
     */
    inline ElementType *Begin() { return start; }

    inline const ElementType *Begin() const { return start; }

    /*
     * End() - Returns an end iterator that is similar to the one for vector
     */
    inline ElementType *End() { return end; }

    inline const ElementType *End() const { return end; }

    /*
     * REnd() - Returns the element before the first element
     *
     * Note that since we returned an invalid pointer into the array, the
     * return value should not be modified and is therefore of const type
     */
    inline const ElementType *REnd() { return start - 1; }

    inline const ElementType *REnd() const { return start - 1; }

    /*
     * GetSize() - Returns the size of the embedded list
     *
     * Note that the return type is integer since we use integer to represent
     * the size of a node
     */
    inline int GetSize() const { return static_cast<int>(End() - Begin()); }

    /*
     * PushBack() - Push back an element
     *
     * This function takes an element type and copy-construct it on the array
     * which is invisible to the compiler. Therefore we must call placement
     * operator new to do the job
     */
    inline void PushBack(const ElementType &element) {
      // Placement new + copy constructor using end pointer
      new (end) ElementType{element};

      // Move it pointing to the enxt available slot, if not reached the end
      end++;
    }

    /*
     * PushBack() - Push back a series of elements
     *
     * The overloaded PushBack() could also push an array of elements
     */
    inline void PushBack(const ElementType *copy_start_p, const ElementType *copy_end_p) {
      // Make sure the loop will come to an end
      TERRIER_ASSERT(copy_start_p <= copy_end_p, "Loop will not come to an end.");

      while (copy_start_p != copy_end_p) {
        PushBack(*copy_start_p);
        copy_start_p++;
      }
    }

   public:
    /*
     * Get() - Static helper function that constructs a elastic node of
     *         a certain size
     *
     * Note that since operator new is only capable of allocating a fixed
     * sized structure, we need to call malloc() directly to deal with variable
     * lengthed node. However, after malloc() returns we use placement operator
     * new to initialize it, such that the node could be freed using operator
     * delete later on
     */
    inline static ElasticNode *Get(int size,  // Number of elements
                                   NodeType p_type, int p_depth,
                                   int p_item_count,  // Usually equal to size
                                   const KeyNodeIDPair &p_low_key, const KeyNodeIDPair &p_high_key) {
      // Currently this is always true - if we want a larger array then
      // just remove this line

      // Allocte memory for
      //   1. AllocationMeta (chunk)
      //   2. node meta
      //   3. ElementType array
      // basic template + ElementType element size * (node size) + CHUNK_SIZE()
      // Note: do not make it constant since it is going to be modified
      // after being returned
      auto *alloc_base = new char[sizeof(ElasticNode) + size * sizeof(ElementType) + AllocationMeta::CHUNK_SIZE()];

      // Initialize the AllocationMeta - tail points to the first byte inside
      // class ElasticNode; limit points to the first byte after class
      // AllocationMeta
      new (reinterpret_cast<AllocationMeta *>(alloc_base))
          AllocationMeta{alloc_base + AllocationMeta::CHUNK_SIZE(), alloc_base + sizeof(AllocationMeta)};

      // The first CHUNK_SIZE() byte is used by class AllocationMeta
      // and chunk data
      auto *node_p = reinterpret_cast<ElasticNode *>(alloc_base + AllocationMeta::CHUNK_SIZE());

      // Call placement new to initialize all that could be initialized
      new (node_p) ElasticNode{p_type, p_depth, p_item_count, p_low_key, p_high_key};

      return node_p;
    }

    /*
     * GetNodeHeader() - Given low key pointer, returns the node header
     *
     * This is useful since only the low key pointer is available from any
     * type of node
     */
    static ElasticNode *GetNodeHeader(const KeyNodeIDPair *low_key_p) {
      static constexpr size_t low_key_offset = offsetof(ElasticNode, low_key);

      return reinterpret_cast<ElasticNode *>(reinterpret_cast<uint64_t>(low_key_p) - low_key_offset);
    }

    /*
     * GetAllocationHeader() - Returns the address of class AllocationHeader
     *                         embedded inside the ElasticNode object
     */
    static AllocationMeta *GetAllocationHeader(const ElasticNode *node_p) {
      return reinterpret_cast<AllocationMeta *>(reinterpret_cast<uint64_t>(node_p) - AllocationMeta::CHUNK_SIZE());
    }

    /*
     * InlineAllocate() - Allocates a delta node in preallocated area preceeds
     *                    the data area of this ElasticNode
     *
     * Note that for any given NodeType, we always know its low key and the
     * low key always points to the struct inside base node. This way, we
     * compute the offset of the low key from the begining of the struct,
     * and then subtract it with CHUNK_SIZE() to derive the address of
     * class AllocationMeta
     *
     * Note that since this function is accessed when the header is unknown
     * so (1) it is static, and (2) it takes low key p which is universally
     * available for all node type (stored in NodeMetadata)
     */
    static void *InlineAllocate(const KeyNodeIDPair *low_key_p, size_t size) {
      const ElasticNode *node_p = GetNodeHeader(low_key_p);
      TERRIER_ASSERT(&node_p->low_key == low_key_p, "low_key is not low_key_p.");

      // Jump over chunk content
      AllocationMeta *meta_p = GetAllocationHeader(node_p);

      void *p = meta_p->Allocate(size);

      return p;
    }

    /*
     * At() - Access element with bounds checking under debug mode
     */
    inline ElementType &At(const int index) {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }

    inline const ElementType &At(const int index) const {
      // The index must be inside the valid range
      TERRIER_ASSERT(index < GetSize(), "Index out of range.");

      return *(Begin() + index);
    }
  };


  /*
   * class InnerNode - Inner node that holds separators
   */
  class InnerNode : public ElasticNode<KeyNodeIDPair> {
   public:
    /*
     * Constructor - Deleted
     *
     * All construction of InnerNode should be through ElasticNode interface
     */
    InnerNode() = delete;
    InnerNode(const InnerNode &) = delete;
    InnerNode(InnerNode &&) = delete;
    InnerNode &operator=(const InnerNode &) = delete;
    InnerNode &operator=(InnerNode &&) = delete;

    /*
     * Destructor - Calls destructor of ElasticNode
     */
    ~InnerNode() { this->~ElasticNode<KeyNodeIDPair>(); }

    /*
     * GetSplitSibling() - Split InnerNode into two halves.
     *
     * This function does not change the current node since all existing nodes
     * should be read-only to avoid data race. It copies half of the inner node
     * into the split sibling, and return the sibling node.
     */
    InnerNode *GetSplitSibling() const {
      // Call function in class ElasticNode to determine the size of the
      // inner node
      int key_num = this->GetSize();

      // Inner node size must be > 2 to avoid empty split node
      // Same reason as in leaf node - since we only split inner node
      // without a delta chain on top of it, the sep list size must equal
      // the recorded item count
      TERRIER_ASSERT(key_num == this->GetItemCount(), "the sep list size must equal the recorded item count");

      int split_item_index = key_num / 2;

      // This is the split point of the inner node
      auto copy_start_it = this->Begin() + split_item_index;

      // We need this to allocate enough space for the embedded array
      auto sibling_size = static_cast<int>(std::distance(copy_start_it, this->End()));

      // This sets metadata inside BaseNode by calling SetMetaData()
      // inside inner node constructor
      auto *inner_node_p = reinterpret_cast<InnerNode *>(ElasticNode<KeyNodeIDPair>::Get(
          sibling_size, NodeType::InnerType, 0, sibling_size, this->At(split_item_index), this->GetHighKeyPair()));

      // Call overloaded PushBack() to insert an array of elements
      inner_node_p->PushBack(copy_start_it, this->End());

      // Since we copy exactly that many elements
      TERRIER_ASSERT(inner_node_p->GetSize() == sibling_size, "Copied number of elements must match.");
      TERRIER_ASSERT(inner_node_p->GetSize() == inner_node_p->GetItemCount(), "Copied number of elements must match.");

      return inner_node_p;
    }
  };

  /*
   * class LeafNode - Leaf node that holds data
   *
   * There are 5 types of delta nodes that could be appended
   * to a leaf node. 3 of them are SMOs, and 2 of them are data operation
   */
  class LeafNode : public ElasticNode<KeyValuePair> {
   public:
    LeafNode() = delete;
    LeafNode(const LeafNode &) = delete;
    LeafNode(LeafNode &&) = delete;
    LeafNode &operator=(const LeafNode &) = delete;
    LeafNode &operator=(LeafNode &&) = delete;

    /*
     * Destructor - Calls underlying ElasticNode d'tor
     */
    ~LeafNode() { this->~ElasticNode<KeyValuePair>(); }

    /*
     * FindSplitPoint() - Find the split point that could divide the node
     *                    into two even siblings
     *
     * If such point does not exist then we manage to find a point that
     * divides the node into two halves that are as even as possible (i.e.
     * make the size difference as small as possible)
     *
     * This function works by first finding the key on the exact central
     * position, after which it scans forward to find a KeyValuePair
     * with a different key. If this fails then it scans backwards to find
     * a KeyValuePair with a different key.
     *
     * NOTE: If both split points would make an uneven division with one of
     * the node size below the merge threshold, then we do not split,
     * and return -1 instead. Otherwise the index of the spliting point
     * is returned
     */
    int FindSplitPoint(const BwTree *t) const {
      int central_index = this->GetSize() / 2;

      // This will used as upper_bound and lower_bound key
      const KeyValuePair &central_kvp = this->At(central_index);

      // Move it to the element before data_list
      auto it = this->Begin() + central_index - 1;

      // If iterator has reached the begin then we know there could not
      // be any split points
      while ((it != this->Begin()) && t->KeyCmpEqual(it->first, central_kvp.first)) {
        it--;
      }

      // This is the real split point
      it++;

      // This size is exactly the index of the split point
      int left_sibling_size = std::distance(this->Begin(), it);

      if (left_sibling_size > static_cast<int>(t->GetLeafNodeSizeLowerThreshold())) {
        return left_sibling_size;
      }

      // Move it to the element after data_list
      it = this->Begin() + central_index + 1;

      // If iterator has reached the end then we know there could not
      // be any split points
      while ((it != this->End()) && t->KeyCmpEqual(it->first, central_kvp.first)) {
        it++;
      }

      int right_sibling_size = std::distance(it, this->End());

      if (right_sibling_size > static_cast<int>(t->GetLeafNodeSizeLowerThreshold())) {
        return std::distance(this->Begin(), it);
      }

      return -1;
    }

    /*
     * GetSplitSibling() - Split the node into two halves
     *
     * Although key-values are stored as independent pairs, we always split
     * on the point such that no keys are separated on two leaf nodes
     * This decision is made to make searching easier since now we could
     * just do a binary search on the base page to decide whether a
     * given key-value pair exists or not
     *
     * This function splits a leaf node into halves based on key rather
     * than items. This implies the number of keys would be even, but no
     * guarantee is made about the number of key-value items, which might
     * be very unbalanced, cauing node size varing much.
     *
     * NOTE: This function allocates memory, and if it is not used
     * e.g. by a CAS failure, then caller needs to delete it
     *
     * NOTE 2: Split key is stored in the low key of the new leaf node
     *
     * NOTE 3: This function assumes no out-of-bound key, i.e. all keys
     * stored in the leaf node are < high key. This is valid since we
     * already filtered out those key >= high key in consolidation.
     *
     * NOTE 4: On failure of split (i.e. could not find a split key that evenly
     * or almost evenly divide the leaf node) then the return value of this
     * function is nullptr
     */
    LeafNode *GetSplitSibling(const BwTree *t) const {
      // When we split a leaf node, it is certain that there is no delta
      // chain on top of it. As a result, the number of items must equal
      // the actual size of the data list
      TERRIER_ASSERT(this->GetSize() == this->GetItemCount(),
                     "the number of items does not equal the actual size of the data list");

      // This is the index of the actual key-value pair in data_list
      // We need to substract this value from the prefix sum in the new
      // inner node
      int split_item_index = FindSplitPoint(t);

      // Could not split because we could not find a split point
      // and the caller is responsible for not spliting the node
      // This should happen relative infrequently, in a sense that
      // oversized page would affect performance
      if (split_item_index == -1) {
        return nullptr;
      }

      // This is an iterator pointing to the split point in the vector
      // note that std::advance() operates efficiently on std::vector's
      // RandomAccessIterator
      auto copy_start_it = this->Begin() + split_item_index;

      // This is the end point for later copy of data
      auto copy_end_it = this->End();

      // This is the key part of the key-value pair, also the low key
      // of the new node and new high key of the current node (will be
      // reflected in split delta later in its caller)
      const KeyType &split_key = copy_start_it->first;

      auto sibling_size = static_cast<int>(std::distance(copy_start_it, copy_end_it));

      // This will call SetMetaData inside its constructor
      auto *leaf_node_p = reinterpret_cast<LeafNode *>(
          ElasticNode<KeyValuePair>::Get(sibling_size, NodeType::LeafType, 0, sibling_size,
                                         std::make_pair(split_key, ~INVALID_NODE_ID), this->GetHighKeyPair()));

      // Copy data item into the new node using PushBack()
      leaf_node_p->PushBack(copy_start_it, copy_end_it);

      TERRIER_ASSERT(leaf_node_p->GetSize() == sibling_size, "Copied number of elements must match.");
      TERRIER_ASSERT(leaf_node_p->GetSize() == leaf_node_p->GetItemCount(), "Copied number of elements must match.");

      return leaf_node_p;
    }
  };

  /***
   * 单线程下遍历树时候存储Key
   *
   * 因为每个线程只能存储一个结点的对象
   * 所以需要禁止copy construction 还有copy assignment
   */
  class Context {
   public:
    inline Context(const KeyType &keyType) : searchKey{keyType}, abort_flag{false} {}

    ~Context() = default;

    Context(const Context &context) = delete;
    Context &operator=(const Context &context) = delete;
    Context(Context &&context) = delete;
    Context &operator=(Context &&context) = delete;

    inline bool isRootNode() { return parentSnapshot.node_id == INVALID_NODE_ID; }

    const KeyType searchKey;
    NodeSnapshot currentSnapshot;
    NodeSnapshot parentSnapshot;
    bool abort_flag;
  };

  // member
  std::atomic<NodeID> root_id;
  std::atomic<const BaseNode *> *mapping_table;

 private:
  // inline
  inline bool InstallNodeToReplace(NodeID node_id, const BaseNode *node_p, const BaseNode *prev_p) {
    // CAS更新mappingtable的
    return mapping_table[node_id].compare_exchange_strong(prev_p, node_p);
  }
  static inline NodeSnapshot *GetLatestNodeSnapshot(Context *context_p) { return &context_p->current_snapshot; }

 private:
  // PRIVATE UTILS FUNCTION
  // bwtree的遍历
  const KeyValuePair Traverse(Context *context, const ValueType *value, std::pair<int, bool> *indexPair,
                              bool isUnique = false) {
    const KeyValuePair *foundPair = nullptr;

  retry_traverse:
    // root node
    NodeID start_node_id = root_id.load();

    context->current_snapshot.node_id = INVALID_NODE_ID;

    //
    LoadNodeID(start_node_id, context);

    if (context->abort_flag) {
      goto abort_traverse;
    }
    // while循环会从root Node开始遍历然后一直找到叶子节点
    while (1) {
      NodeID childNodeId = NavigateInnerNode(context);
      // Navigate could abort since it might go to another NodeID
      // if there is a split delta and the key is >= split key
      if (context->abort_flag) {
        // If NavigateInnerNode() aborts then it returns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        goto abort_traverse;
      }

      LoadNodeID(childNodeId, context);

      if (context->abort_flag) {
        // If NavigateInnerNode() aborts then it returns INVALID_NODE_ID
        // as a double check
        // This is the only situation that this function returns
        // INVALID_NODE_ID
        goto abort_traverse;
      }
      //
      NodeSnapshot *snapshot = GetLatestNodeSnapshot(context);
      if (snapshot->IsLeaf()) {
        INDEX_LOG_TRACE("The next node is a leaf");
        break;
      }
    }  // while (1)

    if (value == nullptr) {
      //如果没有给定value，则只用遍历链表？这里感觉不太明白，可能需要再读一下bwtree的论文
      NavigateSiblingChain(context);
    } else {
      //如果给定了value，则要遍历叶子节点去寻找要找的结点<K,V>是否存在
      foundPair = NavigateLeafNode(context, *value, indexPair, isUnique);
    }

    if (context->abort_flag) {
      goto abort_traverse;
    }

    return foundPair;

  abort_traverse:
    // This is used to identify root node
    context->current_snapshot.node_id = INVALID_NODE_ID;

    context->abort_flag = false;

    goto retry_traverse;

    return nullptr;
  }

 private:
  // Data Storave Core 部分
  void NavigateSiblingChain(Context *context) {
    while (1) {
      NodeSnapshot *snapshot = GetLatestNodeSnapshot(context);
      const BaseNode *node = snapshot->node;

      // verify
      if ((node->GetNextNodeID() != INVALID_NODE_ID) &&
          //被搜索的Node Key范围不能超过父节点
          KepCmpGreaterEqual(context->searchKey, node->GetHighKey())) {
        //把链表下一个结点赋给context
        JumpToNodeID(node->GetNextNodeID(), context);
        if (context->abort_flag) {
          return;
        }
      } else {
        break;
      }
    }
  }

  NodeID NavigateInnerNode(Context *context) {
    //找到一个范围包含所要搜索Key的节点
    NavigateSiblingChain(context);
    if (context->abort_flag) {
      return INVALID_NODE_ID;
    }

    const KeyType &search_key = context->searchKey;
    NodeSnapshot *snapshot = GetLatestNodeSnapshot(context);
    const BaseNode *node_p = snapshot->node;
    const KeyNodeIDPair *start_p = InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->Begin()+1;
    const KeyNodeIDPair *end_p = InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->End();

    while(1) {
      NodeType type = node_p->GetType();
      switch (type) {
        case NodeType::InnerType: {
          const auto *inner_node_p = static_cast<const InnerNode *>(node_p);

          // We always use the ubound recorded inside the top of the
          // delta chain
          NodeID target_id = LocateSeparatorByKey(search_key, inner_node_p, start_p, end_p);

          return target_id;
        }  // case InnerType
        case NodeType::InnerInsertType: {
          const auto *insert_node_p = static_cast<const InnerInsertNode *>(node_p);

          const KeyNodeIDPair &insert_item = insert_node_p->item;
          const KeyNodeIDPair &next_item = insert_node_p->next_item;

          // This comparison servers two purposes:
          //   1. Check whether we could use it to do a quick jump
          //   2. Update start_index or end_index depending on the
          //      result of comparison
          if (KeyCmpGreaterEqual(search_key, insert_item.first)) {
            if ((next_item.second == INVALID_NODE_ID) || (KeyCmpLess(search_key, next_item.first))) {
              return insert_item.second;
            }

            start_p = std::max(start_p, insert_node_p->location);
          } else {
            end_p = std::min(end_p, insert_node_p->location);
          }

          break;
        }  // InnerInsertType
        case NodeType::InnerDeleteType: {
          const auto *delete_node_p = static_cast<const InnerDeleteNode *>(node_p);

          const KeyNodeIDPair &prev_item = delete_node_p->prev_item;
          const KeyNodeIDPair &next_item = delete_node_p->next_item;

          // NOTE: Low key ID will not be changed (i.e. being deleted or
          // being preceded by other key-NodeID pair)
          // If the prev item is the leftmost item then we do not need to
          // compare since we know the search key is definitely greater than
          // or equal to the low key (this is necessary to prevent comparing
          // with -Inf)
          // NOTE: Even if the inner node is merged into its left sibling
          // this is still true since we compared prev_item.second
          // with the low key of the current delete node which always
          // reflects the low key of this branch
          if ((delete_node_p->GetLowKeyNodeID() == prev_item.second) ||
              (KeyCmpGreaterEqual(search_key, prev_item.first))) {
            // If the next item is +Inf key then we also choose not to compare
            // keys directly since we know the search key is definitely smaller
            // then +Inf
            if ((next_item.second == INVALID_NODE_ID) || (KeyCmpLess(search_key, next_item.first))) {
              return prev_item.second;
            }
          }

          // Use the deleted key to do a divide - all keys less than
          // it is on the left of the index recorded in this InnerInsertNode
          // Otherwise it is to the right of it
          if (KeyCmpGreaterEqual(search_key, delete_node_p->item.first)) {
            start_p = std::max(delete_node_p->location, start_p);
          } else {
            end_p = std::min(delete_node_p->location, end_p);
          }

          break;
        }  // InnerDeleteType
        case NodeType::InnerSplitType: {
          break;
        }  // case InnerSplitType
        case NodeType::InnerMergeType: {
          const auto *merge_node_p = static_cast<const InnerMergeNode *>(node_p);

          const KeyType &merge_key = merge_node_p->delete_item.first;

          // Here since we will only take one branch, so
          // high key does not need to be updated
          // Since we still could not know the high key
          if (KeyCmpGreaterEqual(search_key, merge_key)) {
            node_p = merge_node_p->right_merge_p;
          } else {
            node_p = merge_node_p->child_node_p;
          }

          // Since all indices are not invalidated, we do not know on which
          // branch it is referring to
          // After this point node_p has been updated as the newest branch we
          // are travelling on
          start_p = InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->Begin() + 1;
          end_p = InnerNode::GetNodeHeader(&node_p->GetLowKeyPair())->End();

          // Note that we should jump to the beginning of the loop without
          // going to child node any further
          continue;
        }  // InnerMergeType
        default: {
          INDEX_LOG_ERROR("ERROR: Unknown node type = %d", static_cast<int>(type));
        }
      }  // switch type

      node_p = static_cast<const DeltaNode *>(node_p)->child_node_p;
    }  // while 1

    // Should not reach here
    return INVALID_NODE_ID;
  }

  // basic operaation
 public:
  bool Insert(const KeyType &key, const ValueType &value, bool unique_key = false) {
    while (1) {
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

      // CAS更新mapping Table 把快照的结点node_p替换为要插入的结点insert_node_p
      bool ret = InstallNodeToReplace(node_id, insert_node_p, node_p);
      if (ret) {
        INDEX_LOG_TRACE("Leaf Insert delta CAS succeed");

        // If install is a success then just break from the loop
        // and return
        break;
      }
      // destructor
      insert_node_p->~LeafInsertNode();
    }
  }
};

}  // namespace terrier::storage::index
