//
// Created by Morgan Weaver on 4/14/17.
//
#pragma once

#include "heap_storage.h"
#include "schema_tables.h"
#include "SQLParser.h"

class Table {
public:
    Table(Tables* metatable, Identifier table_name, std::vector<hsql::ColumnDefinition*>* columns  );
     ~Table(){}


     void show_columns();
     void create();

     void drop();
     void show();

//     Handle insert(const ValueDict* row);
//     void update(const Handle handle, const ValueDict* new_values);
//     void del(const Handle handle);
//
//     Handles* select();
//     Handles* select(const ValueDict* where);
//     ValueDict* project(Handle handle);
//     ValueDict* project(Handle handle, const ColumnNames* column_names);
private:
    Tables *_metatable;
    std::string name;
    std::vector<hsql::ColumnDefinition*>* column_defs;

};
