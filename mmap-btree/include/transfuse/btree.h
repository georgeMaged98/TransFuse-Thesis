#ifndef INCLUDE_TRANSFUSE_BTREE_H
#define INCLUDE_TRANSFUSE_BTREE_H

#include "iostream"
#include "transfuse/buffer_manager.h"
#include "transfuse/segment.h"
#include <optional>
#include <vector>
#include <string.h>

using namespace std;

namespace transfuse {

template <typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
struct BTree : public Segment {
   struct Node {
      /// The level in the tree.
      uint16_t level;
      /// The number of children.
      uint16_t count;

      // Constructor
      Node(uint16_t level, uint16_t count)
         : level(level), count(count) {}

      /// Is the node a leaf node?
      bool is_leaf() const { return level == 0; }
   };

   struct InnerNode : public Node {
      /// The capacity of a node.
      // Each Node can store (k) keys and (k+1) children
      static constexpr uint32_t levelSize = sizeof(uint16_t);
      static constexpr uint32_t countSize = sizeof(uint16_t);
      static constexpr uint32_t childSize = sizeof(uint64_t);
      static constexpr uint32_t keySize = sizeof(KeyT);

      static constexpr uint32_t kCapacity = (PageSize - childSize - levelSize - countSize) / (keySize + childSize);

      /// The keys.
      KeyT keys[kCapacity];
      /// The values.
      uint64_t children[kCapacity + 1];

      /// Constructor.
      InnerNode() : Node(0, 0) {
      }
      InnerNode(uint16_t level) : Node(level, 0) {}

      /// Get the index of the first key that is not less than than a provided key.
      /// @param[in] key          The key that should be inserted.
      std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
         if (this->count == 0)
            return std::make_pair(0, false);

         ComparatorT comparator;
         uint32_t lb = 0;
         uint32_t ub = this->count - 1;

         while (lb < ub) {
            int mid = lb + (ub - lb) / 2;

            // Check if the middle element is less than the target
            if (comparator(keys[mid], key)) {
               lb = mid + 1;
            } else {
               ub = mid;
            }
         }
         bool isExists = (lb < this->count && keys[lb] == key);
         return std::make_pair(lb, isExists);
      }

      /// Insert a key.
      /// @param[in] key          The key that should be inserted.
      /// @param[in] split_page   The child that should be inserted. The child (value) here is a page id.
      void insert_split(const KeyT& key, uint64_t split_page) {
         std::pair<uint32_t, bool> lowerBound = lower_bound(key);
         uint32_t idx = lowerBound.first;

         if (this->count > 0) {
            for (uint32_t i = this->count - 1; i > idx; i--) {
               keys[i] = keys[i - 1];
               children[i + 1] = children[i];
            }
         }
         keys[idx] = key;
         children[idx + 1] = split_page;

         this->count++;
      }

      /// Split the node.
      /// @param[in] buffer       The buffer for the new page.
      /// @return                 The separator key.
      KeyT split(std::byte* buffer) {
         // determine mid element in current leaf node
         uint32_t mid = this->count / 2;
         // separator is element at mid
         KeyT separator = keys[mid];

         // create new leaf node
         auto* newInnerNode = new (buffer) InnerNode(this->level);
         // insert elements starting mid+1 into the new node
         auto size = this->count;
         newInnerNode->children[0] = children[mid + 1];
         newInnerNode->count = 1;
         for (uint32_t i = mid + 1; i < size - 1; i++) {
            newInnerNode->insert_split(keys[i], children[i + 1]);
         }

         // remove elements starting mid+1 from the current node
         this->count = mid + 1;
         // return max key in the left subtree
         return separator;
      }

      /// Returns the keys.
      std::vector<KeyT> get_key_vector() {
         return std::vector<KeyT>(this->keys.begin(), this->keys.end());
      }
   };

   struct LeafNode : public Node {
      /// The capacity of a node.
      static constexpr uint32_t levelSize = sizeof(uint16_t);
      static constexpr uint32_t countSize = sizeof(uint16_t);
      static constexpr uint32_t childSize = sizeof(ValueT);
      static constexpr uint32_t keySize = sizeof(KeyT);

      static constexpr uint32_t kCapacity = (PageSize - levelSize - countSize - childSize) / (keySize + childSize);

      /// The keys.
      KeyT keys[kCapacity]; // adjust this
      /// The values.
      ValueT values[kCapacity]; // adjust this

      /// Constructor.
      LeafNode() : Node(0, 0) {}

      /// Get the index of the first key that is not less than than a provided key.
      std::pair<uint32_t, bool> lower_bound(const KeyT& key) {
         if (this->count == 0)
            return std::make_pair(0, false);

         ComparatorT comparator;
         uint32_t lb = 0;
         uint32_t ub = this->count;

         while (lb < ub) {
            int mid = lb + (ub - lb) / 2;

            // Check if the middle element is less than the target
            if (comparator(keys[mid], key)) {
               lb = mid + 1;
            } else {
               ub = mid;
            }
         }
         bool isExists = (lb < this->count && keys[lb] == key);
         return std::make_pair(lb, isExists);
      }

      /// Insert a key.
      /// @param[in] key          The key that should be inserted.
      /// @param[in] value        The value that should be inserted.
      void insert(const KeyT& key, const ValueT& value) {
         std::pair<uint32_t, bool> lowerBound = lower_bound(key);
         uint32_t idx = lowerBound.first;

         if (lowerBound.second) {
            values[idx] = value;
            return;
         }

         for (uint32_t i = this->count; i > idx; i--) {
            if (!lowerBound.second)
               keys[i] = keys[i - 1];
            values[i] = values[i - 1];
         }
         keys[idx] = key;
         values[idx] = value;

         this->count++;
      }

      /// Erase a key.
      void erase(const KeyT& key) {
         if (this->count == 0) return;

         std::pair<uint32_t, bool> lowerBound = lower_bound(key);
         uint32_t idx = lowerBound.first;
         // Override the keys
         for (uint32_t i = idx; i < this->count - 1; i++) {
            keys[i] = keys[i + 1];
            values[i] = values[i + 1];
         }
         this->count--;
      }

      /// Split the node.
      /// @param[in] buffer       The buffer for the new page.
      /// @return                 The separator key.
      KeyT split(std::byte* buffer) {
         // determine mid element in current leaf node
         uint32_t mid = this->kCapacity / 2;
         // separator is element at mid
         KeyT separator = keys[mid];

         // create new leaf node
         auto* newLeafNode = new (buffer) LeafNode();
         // insert elements starting mid+1 into the new node
         auto size = this->count;
         for (uint32_t i = mid + 1; i < size; i++) {
            newLeafNode->insert(keys[i], values[i]);
         }
         // remove elements starting mid+1 from the current node
         this->count = mid + 1;
         // return max key in the left subtree
         return separator;
      }

      /// Returns the keys.
      std::vector<KeyT> get_key_vector() {
         return std::vector<KeyT>(this->keys, this->keys + this->count);
      }

      /// Returns the values.
      std::vector<ValueT> get_value_vector() {
         return std::vector<ValueT>(this->values, this->values + this->count);
      }
   };

   /// The root.
   uint64_t root;
   uint64_t num_pages;
   std::shared_mutex latch;

   /// Constructor.
   BTree(uint16_t segment_id, BufferManager& buffer_manager)
      : Segment(segment_id, buffer_manager) {
      root = 0; // root page number
      // Create Leaf Node
      auto& root_page = buffer_manager.fix_page(root, true);

      // Create a new LeafNode
      auto* root_node = new LeafNode();
      memcpy(root_page.get_data(), root_node, sizeof(LeafNode));
      delete root_node;
      buffer_manager.unfix_page(root_page, true);

      num_pages = 1;
   }

   /// Destructor.
   ~BTree() = default;

   /// Lookup an entry in the tree.
   /// @param[in] key      The key that should be searched.
   /// @return             Whether the key was in the tree.
   std::optional<ValueT> lookup(const KeyT& key) {
      latch.lock_shared();

      auto& initial_page = buffer_manager.fix_page(root, false);
      BufferFrame* current_page = &initial_page;
      auto node = reinterpret_cast<BTree::Node*>(current_page->get_data());

      while (!node->is_leaf()) {
         auto inner_node = static_cast<BTree::InnerNode*>(node);
         // Search for the proper leaf node (Tree traversal)
         std::pair<uint32_t, bool> lowerBound = inner_node->lower_bound(key);
         auto pageNr = inner_node->children[lowerBound.first];
         // Unfix the current page before fixing the new one
         buffer_manager.unfix_page(*current_page, false);
         auto& new_page = buffer_manager.fix_page(pageNr, false);
         current_page = &new_page;
         node = reinterpret_cast<BTree::Node*>(current_page->get_data());
      }

      auto leaf_node = static_cast<BTree::LeafNode*>(node);

      std::pair<uint32_t, bool> isKeyFound = leaf_node->lower_bound(key);
      if (isKeyFound.second) {
         buffer_manager.unfix_page(*current_page, false); // Unfix before returning
         auto val = leaf_node->values[isKeyFound.first];
         latch.unlock_shared();
         return val;
      }

      buffer_manager.unfix_page(*current_page, false);
      latch.unlock_shared();
   }

   /// Erase an entry in the tree.
   /// @param[in] key      The key that should be searched.
   void erase(const KeyT& key) {
      latch.lock();
      auto& initial_page = buffer_manager.fix_page(root, true);
      BufferFrame* current_page = &initial_page;
      auto node = reinterpret_cast<BTree::Node*>(current_page->get_data());

      while (!node->is_leaf()) {
         auto inner_node = static_cast<BTree::InnerNode*>(node);
         // Search for the proper leaf node (Tree traversal)
         std::pair<uint32_t, bool> lowerBound = inner_node->lower_bound(key);
         auto pageNr = inner_node->children[lowerBound.first];
         // Unfix the current page before fixing the new one
         buffer_manager.unfix_page(*current_page, true);
         auto& new_page = buffer_manager.fix_page(pageNr, true);
         current_page = &new_page;
         node = reinterpret_cast<BTree::Node*>(current_page->get_data());
      }

      auto leaf_node = static_cast<BTree::LeafNode*>(node);

      std::pair<uint32_t, bool> isKeyFound = leaf_node->lower_bound(key);
      if (!isKeyFound.second) {
         buffer_manager.unfix_page(*current_page, true); // Unfix before returning
         latch.unlock();
         return;
      }

      // Key Exists
      leaf_node->erase(key);

      if (leaf_node->count == 0) { // EMPTY Leaf node
      }
      buffer_manager.unfix_page(*current_page, true);
      latch.unlock();
   }

   pair<uint64_t, uint64_t> splitAndAddParent(Node* node, Node* parentNode, KeyT key, ValueT value) {
      auto right_node_page_id = num_pages++;
      auto& right_node_page = buffer_manager.fix_page(right_node_page_id, true);
      auto right_page_data = reinterpret_cast<std::byte*>(right_node_page.get_data());
      uint64_t parentID = -1;

      KeyT separator;
      if (node->is_leaf()) {
         auto left_node = reinterpret_cast<BTree::LeafNode*>(node);
         separator = left_node->split(right_page_data);
         if (key <= separator) {
            left_node->insert(key, value);
         } else {
            auto new_right_node = reinterpret_cast<BTree::LeafNode*>(right_page_data);
            new_right_node->insert(key, value);
         }
      } else {
         auto left_node = reinterpret_cast<BTree::InnerNode*>(node);
         separator = left_node->split(right_page_data);
      }

      if (parentNode == nullptr) {
         auto parent_node_page_id = num_pages++;
         parentID = parent_node_page_id;
         auto& parent_node_page = buffer_manager.fix_page(parent_node_page_id, true);
         auto parent_page_data = reinterpret_cast<std::byte*>(parent_node_page.get_data());
         auto* in = reinterpret_cast<BTree::InnerNode*>(parent_page_data);
         in->keys[0] = separator;
         in->children[0] = root;
         in->children[1] = right_node_page_id;
         in->count = 2;
         in->level++;
         root = parent_node_page_id;

         buffer_manager.unfix_page(parent_node_page, true);
      } else { // insert new separator in already existing inner Node
         auto* innerParentNode = static_cast<BTree::InnerNode*>(parentNode);
         innerParentNode->insert_split(separator, right_node_page_id);
      }

      buffer_manager.unfix_page(right_node_page, true);
      return {right_node_page_id, parentID};
   }

   /// Inserts a new entry into the tree.
   /// @param[in] key      The key that should be inserted.
   /// @param[in] value    The value that should be inserted.
   void insert(const KeyT& key, const ValueT& value) {
      latch.lock();
      BufferFrame* page = &buffer_manager.fix_page(root, true);
      auto current_node = reinterpret_cast<BTree::Node*>(page->get_data());
      Node* parentNode = nullptr;

      while (!current_node->is_leaf()) {
         // search for proper leaf current_node (Tree traversal)
         auto inner_node = static_cast<BTree::InnerNode*>(current_node);
         if (inner_node->count == inner_node->kCapacity + 1) {
            buffer_manager.unfix_page(*page, true);
            auto newNodeID = this->splitAndAddParent(inner_node, parentNode, 0, 0);

            page = &buffer_manager.fix_page(root, true);
            current_node = reinterpret_cast<BTree::Node*>(page->get_data());
            parentNode = nullptr;
            continue;
         }

         std::pair<uint64_t, bool> lowerBound = inner_node->lower_bound(key);
         buffer_manager.unfix_page(*page, true);

         auto page_id = inner_node->children[lowerBound.first];
         parentNode = inner_node;
         page = &buffer_manager.fix_page(page_id, true);
         current_node = reinterpret_cast<BTree::Node*>(page->get_data());
      }

      auto leaf_node = static_cast<BTree::LeafNode*>(current_node);
      //is there free space on the leaf? --> if yes, insert entry and stop
      if (leaf_node->count < leaf_node->kCapacity) {
         leaf_node->insert(key, value);
         // Unfix Root Page (Old Root page becomes new left node
         buffer_manager.unfix_page(*page, true);
         latch.unlock();
         return;
      }

      // parent == NULL -> CREATE FIRST INNER NODE and make it ROOT
      this->splitAndAddParent(leaf_node, parentNode, key, value);
      //      latch.unlock();
      // Unfix Root Page (Old Root page becomes new left node
      buffer_manager.unfix_page(*page, true);
      latch.unlock();
   }

   // lookup the appropriate leaf page
};

} // namespace transfuse

#endif
