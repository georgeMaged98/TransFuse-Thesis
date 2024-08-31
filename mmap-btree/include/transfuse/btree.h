#ifndef INCLUDE_TRANSFUSE_BTREE_H
#define INCLUDE_TRANSFUSE_BTREE_H

#include "iostream"
#include "transfuse/file_mapper.h"
#include "transfuse/segment.h"
#include <optional>
#include <vector>
#include <cstdint>
#include <shared_mutex>
#include <memory>


using namespace std;

namespace transfuse {
    template<typename KeyT, typename ValueT, typename ComparatorT, size_t PageSize>
    struct BTree : Segment {
        struct Node {
            /// The level in the tree.
            uint16_t level;
            /// The number of children.
            uint16_t count;

            // Constructor
            Node(uint16_t level, uint16_t count)
                : level(level), count(count) {
            }

            /// Is the node a leaf node?
            [[nodiscard]] bool is_leaf() const { return level == 0; }
        };

        struct InnerNode : Node {
            /// The capacity of a node.
            // Each Node can store (k) keys and (k+1) children
            static constexpr uint32_t levelSize = sizeof(uint16_t);
            static constexpr uint32_t countSize = sizeof(uint16_t);
            static constexpr uint32_t childSize = sizeof(uint64_t);
            static constexpr uint32_t keySize = sizeof(KeyT);

            static constexpr uint32_t kCapacity =
                    (PageSize - childSize - levelSize - countSize) / (keySize + childSize);

            /// The keys.
            KeyT keys[kCapacity - 1];
            /// The values.
            uint64_t children[kCapacity]{};

            /// Constructor.
            InnerNode() : Node(0, 0) {
            }

            explicit InnerNode(uint16_t level) : Node(level, 0) {
            }

            /// Get the index of the first key that is not less than than a provided key.
            /// @param[in] key          The key that should be inserted.
            std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
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
            void insert_split(const KeyT &key, uint64_t split_page) {
                std::pair<uint32_t, bool> lowerBound = lower_bound(key);
                uint32_t idx = lowerBound.first;

                if (this->count > 0) {
                    for (uint32_t i = this->count - 1; i > idx; --i) {
                        keys[i] = keys[i - 1];
                        children[i + 1] = children[i];
                    }
                }
                keys[idx] = key;
                children[idx + 1] = split_page;

                ++this->count;
            }

            /// Split the node.
            /// @param[in] buffer       The buffer for the new page.
            /// @return                 The separator key.
            KeyT split(std::byte *buffer) {
                // determine mid element in current leaf node
                uint32_t mid = this->count / 2;
                // separator is element at mid
                KeyT separator = keys[mid];

                // create new leaf node
                auto *newInnerNode = new(buffer) InnerNode(this->level);
                // insert elements starting mid+1 into the new node
                auto size = this->count;
                newInnerNode->children[0] = children[mid + 1];
                newInnerNode->count = 1;
                for (uint32_t i = mid + 1; i < size - 1; ++i) {
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

        struct LeafNode : Node {
            /// The capacity of a node.
            static constexpr uint32_t levelSize = sizeof(uint16_t);
            static constexpr uint32_t countSize = sizeof(uint16_t);
            static constexpr uint32_t childSize = sizeof(ValueT);
            static constexpr uint32_t keySize = sizeof(KeyT);

            static constexpr uint32_t kCapacity =
                    (PageSize - levelSize - countSize - childSize) / (keySize + childSize);

            /// The keys.
            KeyT keys[kCapacity]; // adjust this
            /// The values.
            ValueT values[kCapacity]; // adjust this

            /// Constructor.
            LeafNode() : Node(0, 0) {
            }

            /// Get the index of the first key that is not less than than a provided key.
            std::pair<uint32_t, bool> lower_bound(const KeyT &key) {
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
            void insert(const KeyT &key, const ValueT &value) {
                std::pair<uint32_t, bool> lowerBound = lower_bound(key);
                uint32_t idx = lowerBound.first;

                if (lowerBound.second) {
                    values[idx] = value;
                    return;
                }

                for (uint32_t i = this->count; i > idx; --i) {
                    if (!lowerBound.second)
                        keys[i] = keys[i - 1];
                    values[i] = values[i - 1];
                }
                keys[idx] = key;
                values[idx] = value;

                ++this->count;
            }

            /// Erase a key.
            void erase(const KeyT &key) {
                if (this->count == 0) return;

                std::pair<uint32_t, bool> lowerBound = lower_bound(key);
                uint32_t idx = lowerBound.first;
                // Override the keys
                for (uint32_t i = idx; i < this->count - 1; ++i) {
                    keys[i] = keys[i + 1];
                    values[i] = values[i + 1];
                }
                --this->count;
            }

            /// Split the node.
            /// @param[in] buffer       The buffer for the new page.
            /// @return                 The separator key.
            KeyT split(std::byte *buffer) {
                // determine mid element in current leaf node
                uint32_t mid = kCapacity / 2;
                // separator is element at mid
                KeyT separator = keys[mid];

                // create new leaf node
                auto *newLeafNode = new(buffer) LeafNode();
                // insert elements starting mid+1 into the new node
                auto size = this->count;
                for (uint32_t i = mid + 1; i < size; ++i) {
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
        BTree(const uint16_t segment_id, FileMapper &file_mapper)
            : Segment(segment_id, file_mapper) {
            root = 0; // root page number

            // Create Leaf Node
            const auto *root_page = file_mapper.get_page(root, true);
            /// Create a new LeafNode (Memory Allocation and Placement New)
            /// Placement New: This syntax constructs a LeafNode object at a specific memory address, which is provided by root_page->get_data().
            auto *root_node = new(root_page->get_data()) LeafNode();
            // Ensure that the page data size matches the size of LeafNode
            // if (root_page->get_size() < sizeof(LeafNode)) {
            //     throw std::runtime_error("Page size is smaller than LeafNode size");
            // }
            //delete root_node;
            //file_mapper.unfix_page(root_page, true);

            num_pages = 1;
        }

        /// Destructor.
        ~BTree() = default;


        // [[nodiscard]] Page *read_page(const size_t page_number, const bool is_exclusive) const {
        //     Page *page = file_mapper.get_page(page_number);
        //
        //     page->set_exclusive(is_exclusive);
        //     if (is_exclusive) {
        //         page->latch.lock();
        //     } else {
        //         page->latch.lock_shared();
        //     }
        //     /// Given that your Page class contains a std::shared_mutex, which makes it non-movable and non-copyable,
        //     /// returning the Page object by value or moving it is problematic.
        //     /// To address this, you can return a pointer to the Page object instead.
        //     return page;
        // }

        // void release_page(Page *page) {
        //     if (page->is_exclusive()) {
        //         page->latch.unlock();
        //     } else {
        //         page->latch.unlock_shared();
        //     }
        // }

        /// Lookup an entry in the tree.
        /// @param[in] key      The key that should be searched.
        /// @return             Whether the key was in the tree.
        std::optional<ValueT> lookup(const KeyT &key) {
            latch.lock_shared();

            Page *initial_page = file_mapper.get_page(root, false);
            Page *current_page = initial_page;
            auto node = reinterpret_cast<Node *>(current_page->get_data());

            while (!node->is_leaf()) {
                auto inner_node = reinterpret_cast<InnerNode *>(current_page->get_data());
                // Search for the proper leaf node (Tree traversal)
                std::pair<uint32_t, bool> lowerBound = inner_node->lower_bound(key);
                auto pageNr = inner_node->children[lowerBound.first];
                // Unfix the current page before fixing the new one
                //buffer_manager.unfix_page(*current_page, false);
                Page *new_page = file_mapper.get_page(pageNr, false);
                current_page = new_page;
                node = reinterpret_cast<Node *>(current_page->get_data());
            }

            auto leaf_node = reinterpret_cast<LeafNode *>(current_page->get_data());

            std::pair<uint32_t, bool> isKeyFound = leaf_node->lower_bound(key);
            if (isKeyFound.second) {
                //buffer_manager.unfix_page(*current_page, false); // Unfix before returning
                auto val = leaf_node->values[isKeyFound.first];
                latch.unlock_shared();
                return val;
            }

            //buffer_manager.unfix_page(*current_page, false);
            latch.unlock_shared();
            return std::nullopt;
        }

        /// Erase an entry in the tree.
        /// @param[in] key      The key that should be searched.
        void erase(const KeyT &key) {
            latch.lock();
            Page *initial_page = file_mapper.get_page(root, true);
            Page *current_page = initial_page;
            auto node = reinterpret_cast<Node *>(current_page->get_data());

            while (!node->is_leaf()) {
                auto inner_node = static_cast<InnerNode *>(node);
                // Search for the proper leaf node (Tree traversal)
                std::pair<uint32_t, bool> lowerBound = inner_node->lower_bound(key);
                auto pageNr = inner_node->children[lowerBound.first];
                // Unfix the current page before fixing the new one
                //buffer_manager.unfix_page(*current_page, true);
                auto *new_page = file_mapper.get_page(pageNr, true);
                current_page = new_page;
                node = reinterpret_cast<Node *>(current_page->get_data());
            }

            auto leaf_node = static_cast<LeafNode *>(node);

            std::pair<uint32_t, bool> isKeyFound = leaf_node->lower_bound(key);
            if (!isKeyFound.second) {
                //buffer_manager.unfix_page(*current_page, true); // Unfix before returning
                latch.unlock();
                return;
            }

            // Key Exists
            leaf_node->erase(key);

            if (leaf_node->count == 0) {
                // EMPTY Leaf node
            }
            //buffer_manager.unfix_page(*current_page, true);
            latch.unlock();
        }

        pair<uint64_t, uint64_t> splitAndAddParent(Node *node, Node *parentNode, KeyT key, ValueT value) {
            auto right_node_page_id = num_pages++;
            auto right_node_page = file_mapper.get_page(right_node_page_id, true);
            auto right_page_data = reinterpret_cast<std::byte *>(right_node_page->get_data());
            int64_t parentID = -1;

            KeyT separator;
            if (node->is_leaf()) {
                auto left_node = reinterpret_cast<LeafNode *>(node);
                separator = left_node->split(right_page_data);
                if (key <= separator) {
                    left_node->insert(key, value);
                } else {
                    auto new_right_node = reinterpret_cast<LeafNode *>(right_page_data);
                    new_right_node->insert(key, value);
                }
            } else {
                auto left_node = reinterpret_cast<InnerNode *>(node);
                separator = left_node->split(right_page_data);
            }

            if (parentNode == nullptr) {
                auto parent_node_page_id = num_pages++;
                parentID = parent_node_page_id;
                auto *parent_node_page = file_mapper.get_page(parent_node_page_id, true);
                auto parent_page_data = reinterpret_cast<std::byte *>(parent_node_page->get_data());
                auto *in = reinterpret_cast<InnerNode *>(parent_page_data);
                in->keys[0] = separator;
                in->children[0] = root;
                in->children[1] = right_node_page_id;
                in->count = 2;
                in->level = node->level + 1;
                root = parent_node_page_id;

                //buffer_manager.unfix_page(parent_node_page, true);
            } else {
                // insert new separator in already existing inner Node
                auto *innerParentNode = static_cast<InnerNode *>(parentNode);
                innerParentNode->insert_split(separator, right_node_page_id);

            }

            //buffer_manager.unfix_page(right_node_page, true);
            return {right_node_page_id, parentID};
        }

        /// Inserts a new entry into the tree.
        /// @param[in] key      The key that should be inserted.
        /// @param[in] value    The value that should be inserted.
        void insert(const KeyT &key, const ValueT &value) {
            latch.lock();
            Page *page = file_mapper.get_page(root, true);
            auto current_node = reinterpret_cast<Node *>(page->get_data());
            Node *parentNode = nullptr;

            while (!current_node->is_leaf()) {
                // search for proper leaf current_node (Tree traversal)
                auto inner_node = reinterpret_cast<InnerNode *>(page->get_data());
                if (inner_node->count == inner_node->kCapacity) {
                    //buffer_manager.unfix_page(*page, true);
                    auto newNodeID = this->splitAndAddParent(inner_node, parentNode, 0, 0);

                    page = file_mapper.get_page(root, true);
                    current_node = reinterpret_cast<Node *>(page->get_data());
                    parentNode = nullptr;
                    continue;
                }
                std::pair<uint64_t, bool> lowerBound = inner_node->lower_bound(key);
                //buffer_manager.unfix_page(*page, true);
                auto page_id = inner_node->children[lowerBound.first];
                parentNode = inner_node;

                page = file_mapper.get_page(page_id, true);
                current_node = reinterpret_cast<Node *>(page->get_data());

            }

            auto leaf_node = reinterpret_cast<LeafNode *>(page->get_data());

            //is there free space on the leaf? --> if yes, insert entry and stop
            if (leaf_node->count < leaf_node->kCapacity) {
                leaf_node->insert(key, value);
                // Unfix Root Page (Old Root page becomes new left node
                //buffer_manager.unfix_page(*page, true);
                latch.unlock();
                return;
            }

            // parent == NULL -> CREATE FIRST INNER NODE and make it ROOT
            this->splitAndAddParent(leaf_node, parentNode, key, value);
            //      latch.unlock();
            // Unfix Root Page (Old Root page becomes new left node
            //buffer_manager.unfix_page(*page, true);
            latch.unlock();
        }
    };
} // namespace transfuse

#endif
