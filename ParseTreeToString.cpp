//
// Created by Kevin Lundeen on 4/6/17.
//

#include "ParseTreeToString.h"

std::string ParseTreeToString::operator_expression(const hsql::Expr *expr) {
    if (expr == NULL)
        return "null";

    std::string ret;
    if (expr->opType == hsql::Expr::NOT)
        ret += "NOT ";
    ret += expression(expr->expr) + " ";
    switch (expr->opType) {
        case hsql::Expr::SIMPLE_OP:
            ret += expr->opChar;
            break;
        case hsql::Expr::AND:
            ret += "AND";
            break;
        case hsql::Expr::OR:
            ret += "OR";
            break;
    }
    if (expr->expr2 != NULL)
        ret += " " + expression(expr->expr2);
    return ret;
}

std::string ParseTreeToString::expression(const hsql::Expr *expr) {
    std::string ret;
    switch (expr->type) {
        case hsql::kExprStar:
            ret += "*";
            break;
        case hsql::kExprColumnRef:
            if (expr->table != NULL)
                ret += std::string(expr->table) + ".";
        case hsql::kExprLiteralString:
            ret += expr->name;
            break;
        case hsql::kExprLiteralFloat:
            ret += std::to_string(expr->fval);
            break;
        case hsql::kExprLiteralInt:
            ret += std::to_string(expr->ival);
            break;
        case hsql::kExprFunctionRef:
            ret += std::string(expr->name) + "?" + expr->expr->name;
            break;
        case hsql::kExprOperator:
            ret += operator_expression(expr);
            break;
        default:
            ret += "???";
            break;
    }
    if (expr->alias != NULL)
        ret += std::string(" AS ") + expr->alias;
    return ret;
}

std::string ParseTreeToString::table_ref(const hsql::TableRef *table) {
    std::string ret;
    switch (table->type) {
        case hsql::kTableName:
            ret += table->name;
            if (table->alias != NULL)
                ret += std::string(" AS ") + table->alias;
            break;
        case hsql::kTableJoin:
            ret += table_ref(table->join->left);
            switch (table->join->type) {
                case hsql::kJoinCross:
                case hsql::kJoinInner:
                    ret += " JOIN ";
                    break;
                case hsql::kJoinOuter:
                case hsql::kJoinLeftOuter:
                case hsql::kJoinLeft:
                    ret += " LEFT JOIN ";
                    break;
                case hsql::kJoinRightOuter:
                case hsql::kJoinRight:
                    ret += " RIGHT JOIN ";
                    break;
                case hsql::kJoinNatural:
                    ret += " NATURAL JOIN ";
                    break;
            }
            ret += table_ref(table->join->right);
            if (table->join->condition != NULL)
                ret += " ON " + expression(table->join->condition);
            break;
        case hsql::kTableCrossProduct:
            bool doComma = false;
            for (hsql::TableRef *tbl : *table->list) {
                if (doComma)
                    ret += ", ";
                ret += table_ref(tbl);
                doComma = true;
            }
            break;
    }
    return ret;
}

std::string ParseTreeToString::column_definition(const hsql::ColumnDefinition *col) {
    std::string ret(col->name);
    switch (col->type) {
        case hsql::ColumnDefinition::DOUBLE:
            ret += " DOUBLE";
            break;
        case hsql::ColumnDefinition::INT:
            ret += " INT";
            break;
        case hsql::ColumnDefinition::TEXT:
            ret += " TEXT";
            break;
        default:
            ret += " ...";
            break;
    }
    return ret;
}

std::string ParseTreeToString::select(const hsql::SelectStatement *stmt) {
    std::string ret("SELECT ");
    bool doComma = false;
    for (hsql::Expr *expr : *stmt->selectList) {
        if (doComma)
            ret += ", ";
        ret += expression(expr);
        doComma = true;
    }
    ret += " FROM " + table_ref(stmt->fromTable);
    if (stmt->whereClause != NULL)
        ret += " WHERE " + expression(stmt->whereClause);
    return ret;
}

std::string ParseTreeToString::insert(const hsql::InsertStatement *stmt) {
    return "INSERT ...";
}

std::string ParseTreeToString::create(const hsql::CreateStatement *stmt) {
    std::string ret("CREATE TABLE ");
    if (stmt->type != hsql::CreateStatement::kTable)
        return ret + "...";
    if (stmt->ifNotExists)
        ret += "IF NOT EXISTS ";
    ret += std::string(stmt->tableName) + " (";
    bool doComma = false;
    for (hsql::ColumnDefinition *col : *stmt->columns) {
        if (doComma)
            ret += ", ";
        ret += column_definition(col);
        doComma = true;
    }
    ret += ")";
    return ret;
}

std::string ParseTreeToString::drop(const hsql::DropStatement *stmt) {
    std::string  ret("DROP ");
    switch(stmt->type) {
        case hsql::DropStatement::kTable:
            ret += "TABLE ";
            break;
        default:
            ret += "? ";
    }
    ret += stmt->name;
    return ret;
}

std::string ParseTreeToString::show(const hsql::ShowStatement *stmt) {
    std::string ret("SHOW ");
    switch (stmt->type) {
        case hsql::ShowStatement::kTables:
            ret += "TABLES";
            break;
        case hsql::ShowStatement::kColumns:
            ret += std::string("COLUMNS FROM ") + stmt->tableName;
            break;
        case hsql::ShowStatement::kIndex:
            ret += "INDEX";
            break;
        default:
            ret += "?what?";
            break;
    }
    return ret;
}

std::string ParseTreeToString::statement(const hsql::SQLStatement *stmt) {
    switch (stmt->type()) {
        case hsql::kStmtSelect:
            return select((const hsql::SelectStatement *) stmt);
        case hsql::kStmtInsert:
            return insert((const hsql::InsertStatement *) stmt);
        case hsql::kStmtCreate:
            return create((const hsql::CreateStatement *) stmt);
        case hsql::kStmtDrop:
            return drop((const hsql::DropStatement *) stmt);
        case hsql::kStmtShow:
            return show((const hsql::ShowStatement *) stmt);

        case hsql::kStmtError:
        case hsql::kStmtImport:
        case hsql::kStmtUpdate:
        case hsql::kStmtDelete:
        case hsql::kStmtPrepare:
        case hsql::kStmtExecute:
        case hsql::kStmtExport:
        case hsql::kStmtRename:
        case hsql::kStmtAlter:
        default:
            return "Not implemented";
    }
}
