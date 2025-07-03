#pragma once

#include <vector>
#include <algorithm>
#include <iostream>

using namespace std;

const int ORDER = 4; // Define ORDER constant

template<typename Key, typename Value>
class BPlusTree {
public:
    struct Node {
        vector<Key> keys;
        bool is_leaf;
        Node* parent;
        
        Node(bool leaf = false) : is_leaf(leaf), parent(nullptr) {}
        virtual ~Node() {}
    };
    
    struct LeafNode : public Node {
        vector<Value> values;
        LeafNode* next;
        LeafNode* prev;
        
        LeafNode() : Node(true), next(nullptr), prev(nullptr) {}
    };
    
    struct InternalNode : public Node {
        vector<Node*> children;
        
        InternalNode() : Node(false) {}
        
        ~InternalNode() {
            for (auto child : children) {
                delete child;
            }
        }
    };

private:
    Node* root;
    LeafNode* leftmost_leaf;
    
    LeafNode* find_leaf(const Key& key);
    void insert_in_leaf(LeafNode* leaf, const Key& key, const Value& value);
    Node* split_leaf(LeafNode* leaf);
    Node* split_internal(InternalNode* node);
    void insert_in_parent(Node* left, const Key& key, Node* right);
    void remove_from_leaf(LeafNode* leaf, const Key& key, const Value& value);
    void merge_or_redistribute(Node* node);
    void merge_nodes(Node* node_left, Node* node_right, InternalNode* parent, int sep_idx);
    void redistribute_from_left(Node* node, Node* left_sibling, InternalNode* parent, int node_idx);
    void redistribute_from_right(Node* node, Node* right_sibling, InternalNode* parent, int node_idx);
    int find_child_index(InternalNode* parent, Node* node);
    int min_keys() const;

public:
    BPlusTree();
    ~BPlusTree();

    
    
    void insert(const Key& key, const Value& value);
    vector<Value> search(const Key& key);
    vector<Value> range_search(const Key& start_key, const Key& end_key);
    void remove(const Key& key, const Value& value);

    LeafNode* get_leftmost_leaf() const {
        return leftmost_leaf;
    }
};

// Implementation

template<typename Key, typename Value>
BPlusTree<Key, Value>::BPlusTree() : root(nullptr), leftmost_leaf(nullptr) {}

template<typename Key, typename Value>
BPlusTree<Key, Value>::~BPlusTree() {
    delete root;
}

template<typename Key, typename Value>
typename BPlusTree<Key, Value>::LeafNode* BPlusTree<Key, Value>::find_leaf(const Key& key){
    if(!root) return nullptr;

    Node* current = root;
    while(!current->is_leaf){
        InternalNode* internal = static_cast<InternalNode*>(current);
        int i = 0;
        while(i < current->keys.size() && key >= current->keys[i]) {
            i++;
        }
        current = internal->children[i];
    }
    return static_cast<LeafNode*>(current);
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::insert(const Key& key, const Value& value) {
    if(!root) {
        root = new LeafNode();
        leftmost_leaf = static_cast<LeafNode*>(root);
    }

    LeafNode* leaf = find_leaf(key);
    if(!leaf) {
        leaf = static_cast<LeafNode*>(root);
    }

    insert_in_leaf(leaf, key, value);
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::insert_in_leaf(LeafNode* leaf, const Key& key, const Value& value){
    auto it = lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    int pos = it - leaf->keys.begin();

    // Check if key already exists
    if(it != leaf->keys.end() && *it == key) {
        leaf->values[pos] = value; //update value if key exists
        return;
    }

    //Insert the key and value
    leaf->keys.insert(it, key);
    leaf->values.insert(leaf->values.begin() + pos, value);

    // Check if leaf needs to be split
    if(leaf->keys.size() >= ORDER) {
        Node* new_node = split_leaf(leaf);
        Key split_key = new_node->keys[0];
        // Insert the new key into the parent
        insert_in_parent(leaf, split_key, new_node);
    }
}

template<typename Key, typename Value>
typename BPlusTree<Key,Value>::Node* BPlusTree<Key, Value>::split_leaf(LeafNode* leaf){
    LeafNode* new_leaf = new LeafNode();
    int mid = ORDER / 2;

    //Move half of the keys and values to the new leaf
    new_leaf->keys.assign(leaf->keys.begin() + mid, leaf->keys.end());
    new_leaf->values.assign(leaf->values.begin() + mid, leaf->values.end());

    // Update the original leaf
    leaf->keys.resize(mid);
    leaf->values.resize(mid);

    // Update next pointers
    new_leaf->next = leaf->next;
    leaf->next = new_leaf;

    return new_leaf;
}

template<typename Key, typename Value>
typename BPlusTree<Key, Value>::Node* BPlusTree<Key, Value>::split_internal(InternalNode* node) {
    InternalNode* new_internal = new InternalNode();
    int mid = ORDER / 2;

    // Move keys and children
    new_internal->keys.assign(node->keys.begin() + mid, node->keys.end());
    new_internal->children.assign(node->children.begin() + mid, node->children.end());

    // Update the parent pointers
    for(auto child : new_internal->children) {
        child->parent = new_internal;
    }

    // Get the middle key to promote
    Key split_key = node->keys[mid];

    // Update the original internal node
    node->keys.resize(mid);
    node->children.resize(mid + 1); // +1 because internal nodes have one more

    return new_internal;
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::insert_in_parent(Node* left, const Key& key, Node* right) {
    if(left == root){
        // Create a new root
        InternalNode* new_root = new InternalNode();
        new_root->keys.push_back(key);
        new_root->children.push_back(left);
        new_root->children.push_back(right);
        left->parent = new_root;
        right->parent = new_root;
        root = new_root;
        return;
    }

    InternalNode* parent = static_cast<InternalNode*>(left->parent);
    right->parent = parent;

    // Find the position to insert the new key
    auto it = lower_bound(parent->keys.begin(), parent->keys.end(), key);
    int pos = it - parent->keys.begin();

    parent->keys.insert(it, key);
    parent->children.insert(parent->children.begin() + pos + 1, right);

    // Check if the parent needs to be split
    if(parent->keys.size() >= ORDER){
        Node* new_internal = split_internal(parent);
        Key promote_key = parent->keys.back();
        parent->keys.pop_back(); // Remove the promoted key from the parent
        insert_in_parent(parent, promote_key, new_internal);
    }
}

template<typename Key, typename Value>
vector<Value> BPlusTree<Key, Value>::search(const Key& key){
    vector<Value> result;
    LeafNode* leaf = find_leaf(key);
    if (!leaf) return result;

    auto it = lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it != leaf->keys.end() && *it == key) {
        int pos = it - leaf->keys.begin();
        result.push_back(leaf->values[pos]);
    }

    return result;
}

template<typename Key, typename Value>
vector<Value> BPlusTree<Key, Value>::range_search(const Key& start_key, const Key& end_key){
    vector<Value> result;

    if (start_key > end_key) return result;

    LeafNode* current = find_leaf(start_key);
    if (!current) return result;

    auto start_it = lower_bound(current->keys.begin(), current->keys.end(), start_key);
    int start_pos = start_it - current->keys.begin();

    while (current) {
        for (int i = start_pos; i < current->keys.size(); ++i) {
            if (current->keys[i] > end_key) {
                return result;
            }
            result.push_back(current->values[i]);
        }
        current = current->next;
        start_pos = 0;
    }

    return result;
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::remove(const Key& key, const Value& value) {
    LeafNode* leaf = find_leaf(key);
    if (!leaf) return;

    remove_from_leaf(leaf, key, value);
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::remove_from_leaf(LeafNode* leaf, const Key& key, const Value& value) {
    auto it = std::lower_bound(leaf->keys.begin(), leaf->keys.end(), key);
    if (it != leaf->keys.end() && *it == key) {
        int pos = it - leaf->keys.begin();
        if (leaf->values[pos] == value) {
            leaf->keys.erase(leaf->keys.begin() + pos);
            leaf->values.erase(leaf->values.begin() + pos);

            if (leaf == root) {
                if (leaf->keys.empty()) {
                    delete root;
                    root = nullptr;
                    leftmost_leaf = nullptr;
                }
                return;
            }

            if (leaf->keys.size() < min_keys()) {
                merge_or_redistribute(leaf);
            }
        }
    }
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::merge_or_redistribute(Node* node) {
    if (node == root && node->keys.empty()) {
        if (!node->is_leaf) {
            InternalNode* internal = static_cast<InternalNode*>(node);
            if (!internal->children.empty()) {
                root = internal->children[0];
                root->parent = nullptr;
                internal->children.clear();
            }
        } else {
            root = nullptr;
            leftmost_leaf = nullptr;
        }
        delete node;
        return;
    }

    InternalNode* parent = static_cast<InternalNode*>(node->parent);
    int node_idx = find_child_index(parent, node);

    // Try left sibling
    Node* left_sibling = (node_idx > 0) ? parent->children[node_idx - 1] : nullptr;

    if (left_sibling && left_sibling->keys.size() > min_keys()) {
        redistribute_from_left(node, left_sibling, parent, node_idx);
        return;
    }

    // Try right sibling
    Node* right_sibling = (node_idx < parent->children.size() - 1) ? parent->children[node_idx + 1] : nullptr;

    if (right_sibling && right_sibling->keys.size() > min_keys()) {
        redistribute_from_right(node, right_sibling, parent, node_idx);
        return;
    }

    // Merge
    if (left_sibling) {
        merge_nodes(left_sibling, node, parent, node_idx - 1);
    } else if (right_sibling) {
        merge_nodes(node, right_sibling, parent, node_idx);
    }
}

template<typename Key, typename Value>
int BPlusTree<Key, Value>::min_keys() const {
    return (ORDER + 1) / 2 - 1;  // min number of keys
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::merge_nodes(Node* node_left, Node* node_right, InternalNode* parent, int sep_idx) {
    if (node_left->is_leaf) {
        LeafNode* left = static_cast<LeafNode*>(node_left);
        LeafNode* right = static_cast<LeafNode*>(node_right);
        left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
        left->values.insert(left->values.end(), right->values.begin(), right->values.end());
        left->next = right->next;
        if (right->next) right->next->prev = left;
    } else {
        InternalNode* left = static_cast<InternalNode*>(node_left);
        InternalNode* right = static_cast<InternalNode*>(node_right);
        left->keys.push_back(parent->keys[sep_idx]);
        left->keys.insert(left->keys.end(), right->keys.begin(), right->keys.end());
        left->children.insert(left->children.end(), right->children.begin(), right->children.end());
        for (auto child : right->children) {
            child->parent = left;
        }
    }

    parent->keys.erase(parent->keys.begin() + sep_idx);
    parent->children.erase(parent->children.begin() + sep_idx + 1);
    delete node_right;

    if (parent == root && parent->keys.empty()) {
        root = parent->children[0];
        root->parent = nullptr;
        delete parent;
    } else if (parent->keys.size() < min_keys()) {
        merge_or_redistribute(parent);
    }
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::redistribute_from_left(Node* node, Node* left_sibling, InternalNode* parent, int node_idx) {
    if (node->is_leaf) {
        LeafNode* leaf = static_cast<LeafNode*>(node);
        LeafNode* left = static_cast<LeafNode*>(left_sibling);
        leaf->keys.insert(leaf->keys.begin(), left->keys.back());
        leaf->values.insert(leaf->values.begin(), left->values.back());
        left->keys.pop_back();
        left->values.pop_back();
        parent->keys[node_idx - 1] = leaf->keys[0];
    } else {
        InternalNode* internal = static_cast<InternalNode*>(node);
        InternalNode* left = static_cast<InternalNode*>(left_sibling);
        internal->keys.insert(internal->keys.begin(), parent->keys[node_idx - 1]);
        parent->keys[node_idx - 1] = left->keys.back();
        internal->children.insert(internal->children.begin(), left->children.back());
        left->children.back()->parent = internal;
        left->keys.pop_back();
        left->children.pop_back();
    }
}

template<typename Key, typename Value>
void BPlusTree<Key, Value>::redistribute_from_right(Node* node, Node* right_sibling, InternalNode* parent, int node_idx) {
    if (node->is_leaf) {
        LeafNode* leaf = static_cast<LeafNode*>(node);
        LeafNode* right = static_cast<LeafNode*>(right_sibling);
        leaf->keys.push_back(right->keys.front());
        leaf->values.push_back(right->values.front());
        right->keys.erase(right->keys.begin());
        right->values.erase(right->values.begin());
        parent->keys[node_idx] = right->keys[0];
    } else {
        InternalNode* internal = static_cast<InternalNode*>(node);
        InternalNode* right = static_cast<InternalNode*>(right_sibling);
        internal->keys.push_back(parent->keys[node_idx]);
        parent->keys[node_idx] = right->keys.front();
        internal->children.push_back(right->children.front());
        right->children.front()->parent = internal;
        right->keys.erase(right->keys.begin());
        right->children.erase(right->children.begin());
    }
}

template<typename Key, typename Value>
int BPlusTree<Key, Value>::find_child_index(InternalNode* parent, Node* node) {
    for (int i = 0; i < parent->children.size(); ++i) {
        if (parent->children[i] == node) return i;
    }
    return -1; // should not happen if tree is correct
}