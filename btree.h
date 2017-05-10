#pragma once

#include "BTreeNode.h"

class BTreeIndex : public DbIndex {
public:
    BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique);
    virtual ~BTreeIndex();

    virtual void create();
    virtual void drop();

    virtual void open();
    virtual void close();

    virtual Handles* lookup(ValueDict* key);
    virtual Handles* range(ValueDict* min_key, ValueDict* max_key);

    virtual void insert(Handle handle);
    virtual void del(Handle handle);

    virtual KeyValue *tkey(const ValueDict *key) const; // pull out the key values from the ValueDict in order

protected:
    static const BlockID STAT = 1;
    bool closed;
    BTreeStat *stat;
    BTreeNode *root;
    HeapFile file;
    KeyProfile key_profile;

    void build_key_profile();
    Handles* _lookup(BTreeNode *node, uint height, const KeyValue* key);
    Insertion _insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle);
    BTreeNode *find(BTreeInterior *node, uint height, const KeyValue* key);
    virtual BTreeLeafBase *make_leaf(BlockID id, bool create);
};

class BTreeTable : public DbRelation {
public:
    BTreeTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes,
               const ColumnNames& primary_key);
    virtual ~BTreeTable() {}

    virtual void create();
    virtual void create_if_not_exists();
    virtual void drop();

    virtual void open();
    virtual void close();

    virtual Handle insert(const ValueDict* row);
    virtual void update(const Handle handle, const ValueDict* new_values);
    virtual void del(const Handle handle);

    virtual Handles* select();
    virtual Handles* select(const ValueDict* where);
    virtual Handles* select(Handles *current_selection, const ValueDict* where);

    virtual ValueDict* project(Handle handle);
    virtual ValueDict* project(Handle handle, const ColumnNames* column_names);
    using DbRelation::project;
};

bool test_btree();
