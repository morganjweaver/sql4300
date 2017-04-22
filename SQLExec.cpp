//
// Created by Kevin Lundeen on 4/6/17.
//

#include "SQLExec.h"
#include "Table.h"

std::ostream &operator<<(std::ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << std::endl << "+";
        for (u_long i = 0; i < qres.column_names->size(); i++)
            out << "------------+";
        out << std::endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << std::endl;
        }
    }
    out << qres.message;
    return out;
}

QueryResult::~QueryResult() { /* FIXME */ }

QueryResult *SQLExec::execute(const hsql::SQLStatement *statement) throw(SQLExecError) {
    try {
        switch (statement->type()) {
            case hsql::kStmtCreate:
                return create((const hsql::CreateStatement *) statement);
            case hsql::kStmtDrop:
                return drop((const hsql::DropStatement *) statement);
            case hsql::kStmtShow:
                return show((const hsql::ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(std::string("DbRelationError: ") + e.what());
    }
}


QueryResult *SQLExec::create(const hsql::CreateStatement *statement) {
    Tables tables;
    Columns cols;
    Table temp(&tables, std::string(statement->tableName), statement->columns);
    temp.create(); //populate metatable

    std::cout << statement->tableName << std::endl;
    return new QueryResult("successfully created column ");
}

QueryResult *SQLExec::drop(const hsql::DropStatement *statement) {
    Tables tbls;
    Columns cols;
    bool dropped = false;
    int i = 0;
    Handles* hands = tbls.select(); //create ValueDict of all the row handles
    Handles* col_hands = cols.select(); //and col handles
    while(!dropped && i < hands->size()) {
        Value rowname = tbls.project((*hands)[i])->at("table_name"); //grab all the rows in the column
        if (rowname.s == statement->name) {
            tbls.del((*hands)[i]); //remove metadata
            dropped = true;
        }
        i++;
            }
    i = 0;

    while(i < col_hands->size()) {
        Value rowname = cols.project((*col_hands)[i])->at("table_name"); //grab all the rows in the column
        if (rowname.s == statement->name) {
            cols.del((*col_hands)[i]); //remove metadata
    } i++;
        }
    if (dropped){
        return new QueryResult("table drop success");
    }
    return new QueryResult("table drop fail");
}

QueryResult *SQLExec::show(const hsql::ShowStatement *statement)  {
    Tables tables;
    Columns cols;
    if(statement->type == hsql::ShowStatement::kTables ){
        tables.show_tables();
    } else if(statement->type == hsql::ShowStatement::kColumns ){
        cols.show_cols(statement->tableName);
    } else throw SQLExecError (std::string("Plz request table names or columns"));

    //QueryResult *result = new QueryResult(statement->kColumns, statement->)

    return new QueryResult("show successful");
}
