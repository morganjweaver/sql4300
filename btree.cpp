//
// Created by Morgan Weaver on 5/2/17.
//
#include "btree.h"
#include "SQLExec.h"
#include <typeinfo>


BTreeIndex::BTreeIndex(DbRelation& relation, Identifier name, ColumnNames key_columns, bool unique) :
        DbIndex(relation, name, key_columns, unique), file(name) {

        if(!unique){
            throw DbRelationError("Must be unique");
        }
        this->name = name;
        this->key_columns = key_columns;
        this->closed = true;
        this->relation = relation;
        this->build_key_profile();

}

BTreeIndex::~BTreeIndex() {

}


 void BTreeIndex::create(){
     this->file.create();
     this->stat = new BTreeStat(this->file, STAT, STAT+1, key_profile ); //using extended constructor here and one
     this->root = new BTreeLeaf(this->file, this->stat->get_root_id(), key_profile, true);  //without second stat val in open
     this->closed = false;
//       if(this->closed)
//           this->file.open();
     try {
         for (auto &handle : *this->relation.select()) {
             this->insert(handle);
         }
     } catch(DbBlockNoRoomError){
         file.drop();
         throw;
     }
 };

 void BTreeIndex::drop(){
     this->file.drop();//is this correct?
 };

 void BTreeIndex::open(){
     if (closed) {
         closed = false;
        this->file.open();
         stat = new BTreeStat(file, this->STAT, key_profile);
     }
     if (stat->HEIGHT==1){
         root = new BTreeLeaf(file, stat->get_root_id() ,key_profile,true);
     } else{
         root = new BTreeInterior(file, stat->get_root_id(), key_profile, true);
     }
     closed = false;

 };
 void BTreeIndex::close(){
     file.close();
     stat = nullptr;
     closed = true;
 };

 Handles* BTreeIndex::lookup(ValueDict* key) const{//Find all the rows whose columns are equal to key.
     // Assumes key is a dictionary whose keys are the column names in the index. Returns a list of row handles.

     KeyValue *keyVal = tkey(key);
     Handles *hands = _lookup(this->root, stat->HEIGHT, keyVal);//turn key valueDict into keyValue type with tkey
     return hands;
 }

void BTreeIndex::insert(Handle handle){
    ValueDict *row = relation.project(handle, &key_columns); //existing row: first, get row
    KeyValue *key = tkey(row);  //next, get the values vector
    delete(row);
    Insertion split_root = _insert(this->root, this->stat->get_height() , key, handle );

    if(! this->root->insertion_is_none(split_root)){ //if root was SPLIT, add a level to the tree

        Insertion newIns; //potential copy bug here; copy boundary and address to new Insertion object
        //Insertion: BlockID/KeyVal Pair, same as pointers/boundaries pair types
        BlockID bid = split_root.first;
       KeyValue boundary = split_root.second;
        BTreeInterior *NewRoot = new BTreeInterior(file, 0, key_profile ,true); //replace root
        NewRoot->set_first(root->get_id());
        NewRoot->insert(&boundary, bid); //attach old root to new root one level up
        NewRoot->save();
        stat->set_root_id(NewRoot->get_id());
        stat->set_height(stat->get_height() + 1);
        this->root = NewRoot;
    } //Pretty much exact imp of Python version; hopefully this heap of kludge doesn't explode
}

Insertion BTreeIndex::_insert(BTreeNode *node, uint height, const KeyValue* key, Handle handle){

    if(height == 1 ){
        BTreeLeaf *temp = (BTreeLeaf*)node;
        return temp->insert(key, handle);
    } else{
        Insertion newKid = _insert(dynamic_cast<BTreeInterior*>(node)->find(key, height), height-1, key, handle); //Is height-1 correct!?
        if(!BTreeNode::insertion_is_none(newKid)){
            Insertion temp = dynamic_cast<BTreeInterior*>(node)->insert(&newKid.second, newKid.first);
            return temp;
        }
    }
    return BTreeNode::insertion_none();
}

 Handles* BTreeIndex::range(ValueDict* min_key, ValueDict* max_key) const{
     throw "Not yet implemented";
 }; //no need to implement
 void BTreeIndex::del(Handle handle){
     throw "Not yet implemented";
 }; //do not implement

KeyValue* BTreeIndex::tkey(const ValueDict *key) const{
    KeyValue *vals = new KeyValue;
    for (auto& identifier : key_columns){
        vals->push_back(key->at(identifier));
    }
    return vals;
} // pull out the key values from the ValueDict in order; KeyValue is just a vector for Values.

//Figure out the data types of each key component and encode them in key_profile, a vector of data types; INT, TEXT, BOOL
void BTreeIndex::build_key_profile(){
    ColumnAttributes *attribs = this->relation.get_column_attributes(this->key_columns);
    for (auto& k : *attribs){
        key_profile.push_back(k.get_data_type() );
    }
};

Handles* BTreeIndex::_lookup(BTreeNode *node, uint height, const KeyValue* key) const{
    if(height == 1){ //IF LEAF
        Handles *hands;
        Handle hand = dynamic_cast<BTreeLeaf*>(node)->find_eq(key);
        hands->push_back(hand);
        return hands;

    } else{
        return _lookup(dynamic_cast<BTreeInterior*>(node)->find(key, height), height-1, key);
    }
}

void test_set_row(ValueDict& n_row, int a, int b, int c){

    n_row["ID"] = Value(a);
    n_row["Meows"] = Value(b);
    n_row["Age"] = Value(c);
}

    bool test_btree() {

        ColumnNames *cols = new ColumnNames();
        Identifier id1 = "ID";
        Identifier id2 = "Meows";
        Identifier id3 = "Age";
        cols->push_back(id1);
        cols->push_back(id2);
        cols->push_back(id3);
        ColumnAttributes *attribs = new ColumnAttributes();
        ColumnAttribute c1 = ColumnAttribute::INT;
        ColumnAttribute c2 = ColumnAttribute::INT;
        ColumnAttribute c3 = ColumnAttribute::INT;
        attribs->push_back(c1);
        attribs->push_back(c2);
        attribs->push_back(c3);

        HeapTable *testTbl = new HeapTable("Laser_Cats", *cols, *attribs);
        testTbl->create_if_not_exists();
        testTbl->open();
        ValueDict row;
        ValueDict row1;
        row1["ID"] = 1000;
        row1["Meows"] = 666;
        row1["Age"] = 1;
        ValueDict row2;
        row2["ID"] = 1010;
        row2["Meows"] = 99;
        row2["Age"] = 4;
        testTbl->insert(&row1);
        testTbl->insert(&row2);
        for (int i = 0; i < 1000; i++) {
            test_set_row(row, i, -i, i % 34);
            testTbl->insert(&row);
        }
        std::cout << "Inserted 1002 rows" << std::endl;
        BTreeIndex *test = new BTreeIndex(*testTbl, "LaserCatBtree", *cols, true);
        std::cout << "Instantiated BTree" << std::endl;
        //test->create();
        std::cout << "Created BTree" << std::endl;
        test->open();
        KeyValue *kvtest = new KeyValue();
        kvtest = test->tkey(&row1);
        std::cout<<"TKEY TEST CONVERT TO VAL VECT " << std::endl;
        for (Value v : *kvtest){
            std::cout<< v.n << std::endl;
        }


        ValueDict key;
        key["ID"] = 1000;
        key["Meows"] = 666;
        key["Age"] = 1;

        //project returns valuedict; lookup returns handleS vector, project TAKES handles vector
        //the below feeds the key to the test table to see if the handle corresponds to the row
        ValueDicts *res = testTbl->project(test->lookup(&key));
        std::cout << "Row returned " << std::endl;

        for (int i = 0; i < res->size(); i++) {
            ValueDict tmp;
            std::cout << res->at(i)->at("ID").n << " " << res->at(i)->at("Meows").n << res->at(i)->at("Age").n
                      << std::endl;
            res->pop_back();

        }
        //test->close();
        testTbl->close();
        return true;
    }


















