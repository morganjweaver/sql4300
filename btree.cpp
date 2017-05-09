
#include "btree.h"


BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique)
        : DbIndex(relation, name, key_columns, unique),
          stat(nullptr),
          root(nullptr),
          closed(true),
          file(relation.get_table_name() + "-" + name),
          key_profile() {
    if (!unique)
        throw DbRelationError("BTree index must have unique key");
    build_key_profile();
}

BTreeIndex::~BTreeIndex() {
    delete this->stat;
    delete this->root;
}

// Create the index.
void BTreeIndex::create() {
    this->file.create();
    this->stat = new BTreeStat(this->file, STAT, STAT + 1, this->key_profile);
    this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, true);
    this->closed = false;

    Handles *handles = nullptr;
    try {
        // now build the index! -- add every row from relation into index
        //this->file.begin_write();
        handles = this->relation.select();
        for (auto const &handle: *handles)
            insert(handle);
        //this->file.end_write();
        delete handles;
    } catch(...) {
        delete handles;
        drop();
        throw;
    }
}

// Drop the index.
void BTreeIndex::drop() {
    this->file.drop();
    this->closed = true;
}

// Open existing index. Enables: lookup, range, insert, delete, update.
void BTreeIndex::open() {
    if (this->closed) {
        this->file.open();
        this->stat = new BTreeStat(this->file, STAT, this->key_profile);
        if (this->stat->get_height() == 1)
            this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), this->key_profile, false);
        else
            this->root = new BTreeInterior(this->file, this->stat->get_root_id(), this->key_profile, false);
        this->closed = false;
    }
}

// Closes the index. Disables: lookup, range, insert, delete, update.
void BTreeIndex::close() {
    this->file.close();
    delete this->stat;
    this->stat = nullptr;
    delete this->root;
    this->root = nullptr;
    this->closed = true;
}

// Find all the rows whose columns are equal to key. Assumes key is a dictionary whose keys are the column
// names in the index. Returns a list of row handles.
Handles* BTreeIndex::lookup(ValueDict* key_dict) {
    KeyValue *key = tkey(key_dict);
    Handles *handles = _lookup(this->root, this->stat->get_height(), key);
    delete key;
    return handles;
}

// Recursive lookup.
Handles* BTreeIndex::_lookup(BTreeNode *node, uint depth, const KeyValue* key) {
    if (depth == 1) { // base case: leaf
        BTreeLeaf *leaf = (BTreeLeaf *) node;
        Handles *handles = new Handles();
        try {
            Handle handle = leaf->find_eq(key);
            handles->push_back(handle);
        } catch (std::out_of_range &e) {
            ; // not found, so we return an empty list
        }
        return handles;
    } else { // interior node: find the block to go to in the next level down and recurse there
        BTreeInterior *interior = (BTreeInterior *) node;
        return _lookup(find(interior, depth, key), depth - 1, key);
    }
}

Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) {
    throw DbRelationError("Don't know how to do a range query on Btree index yet");
    // FIXME
}

// Insert a row with the given handle. Row must exist in relation already.
void BTreeIndex::insert(Handle handle) {
    ValueDict *row = this->relation.project(handle, &this->key_columns);
    KeyValue *key = tkey(row);
    delete row;

    Insertion split_root = _insert(this->root, this->stat->get_height(), key, handle);
    if (!BTreeNode::insertion_is_none(split_root)) {
        // if we split the root grow the tree up one level
        BlockID rroot = split_root.first;
        KeyValue boundary = split_root.second;
        BTreeInterior *root = new BTreeInterior(this->file, 0, this->key_profile, true);
        root->set_first(this->root->get_id());
        root->insert(&boundary, rroot);
        root->save();
        this->stat->set_root_id(root->get_id());
        this->stat->set_height(this->stat->get_height() + 1);
        this->stat->save();
        this->root = root;
    }
}

// Recursive insert. If a split happens at this level, return the (new node, boundary) of the split.
Insertion BTreeIndex::_insert(BTreeNode *node, uint depth, const KeyValue* key, Handle handle) {
    if (depth == 1) {
        BTreeLeaf *leaf = (BTreeLeaf *)node;
        return leaf->insert(key, handle);
    } else {
        BTreeInterior *interior = (BTreeInterior *)node;
        Insertion new_kid = _insert(find(interior, depth, key), depth - 1, key, handle);
        if (!BTreeNode::insertion_is_none(new_kid)) {
            BlockID nnode = new_kid.first;
            KeyValue boundary = new_kid.second;
            return interior->insert(&boundary, nnode);
        }
        return BTreeNode::insertion_none();
    }
}

// Call the interior node's find method and construct an appropriate BTreeNode at the next level with the response
BTreeNode *BTreeIndex::find(BTreeInterior *node, uint height, const KeyValue* key)  {
    BlockID down = node->find(key);
    if (height == 2)
        return make_leaf(down);
    else
        return new BTreeInterior(this->file, down, this->key_profile, false);
}

// Construct an appropriate leaf
BTreeLeaf *BTreeIndex::make_leaf(BlockID id) {
    return new BTreeLeaf(this->file, id, this->key_profile, false);
}

// Delete an index entry
void BTreeIndex::del(Handle handle) {
    throw DbRelationError("Don't know how to delete from a BTree index yet");
    // FIXME
}

// Figure out the data types of each key component and encode them in self.key_profile
void BTreeIndex::build_key_profile() {
    ColumnAttributes *key_attributes = this->relation.get_column_attributes(this->key_columns);
    for (auto& ca: *key_attributes)
        key_profile.push_back(ca.get_data_type());
    delete key_attributes;
}

KeyValue *BTreeIndex::tkey(const ValueDict *key) const {
    KeyValue *kv = new KeyValue();
    for (auto& col_name: this->key_columns)
        kv->push_back(key->at(col_name));
    return kv;
}

BTreeTable::BTreeTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes,
                       const ColumnNames& primary_key)
        : DbRelation(table_name, column_names, column_attributes, primary_key) {
    throw DbRelationError("Btree Table not implemented"); // FIXME
}

void BTreeTable::create() {}
void BTreeTable::create_if_not_exists() {}
void BTreeTable::drop() {}
void BTreeTable::open() {}
void BTreeTable::close() {}
Handle BTreeTable::insert(const ValueDict* row) { return Handle(); }
void BTreeTable::update(const Handle handle, const ValueDict* new_values) {}
void BTreeTable::del(const Handle handle) {}
Handles* BTreeTable::select() { return nullptr; }
Handles* BTreeTable::select(const ValueDict* where) { return nullptr; }
Handles* BTreeTable::select(Handles *current_selection, const ValueDict* where) { return nullptr; }
ValueDict* BTreeTable::project(Handle handle) {return nullptr; }
ValueDict* BTreeTable::project(Handle handle, const ColumnNames* column_names) { return nullptr; }


bool test_btree() {
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    column_attributes.push_back(ColumnAttribute(ColumnAttribute::INT));
    HeapTable table("__test_btree", column_names, column_attributes);
    table.create();
    ValueDict row1, row2;
    row1["a"] = Value(12);
    row1["b"] = Value(99);
    row2["a"] = Value(88);
    row2["b"] = Value(101);
    table.insert(&row1);
    table.insert(&row2);
    for (int i = 0; i < 1000; i++) {
        ValueDict row;
        row["a"] = Value(i + 100);
        row["b"] = Value(-i);
        table.insert(&row);
    }
    column_names.clear();
    column_names.push_back("a");
    BTreeIndex index(table, "fooindex", column_names, true);
    index.create();
    ValueDict lookup;
    lookup["a"] = 12;
    Handles *handles = index.lookup(&lookup);
    ValueDict *result = table.project(handles->back());
    if (*result != row1) {
        std::cout << "first lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 88;
    handles = index.lookup(&lookup);
    result = table.project(handles->back());
    if (*result != row2) {
        std::cout << "second lookup failed" << std::endl;
        return false;
    }
    delete handles;
    delete result;
    lookup["a"] = 6;
    handles = index.lookup(&lookup);
    if (handles->size() != 0) {
        std::cout << "third lookup failed" << std::endl;
        return false;
    }
    delete handles;

    for (uint j = 0; j < 10; j++)
        for (int i = 0; i < 1000; i++) {
            lookup["a"] = i + 100;
            handles = index.lookup(&lookup);
            result = table.project(handles->back());
            row1["a"] = i + 100;
            row1["b"] = -i;
            if (*result != row1) {
                std::cout << "lookup failed " << i << std::endl;
                return false;
            }
            delete handles;
            delete result;
        }
    index.drop();
    table.drop();
    return true;
}
