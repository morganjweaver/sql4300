//============================================================================
// Name        : cpcs4300.cpp
// Author      : Kevin Lundeen
// Version     :
// Copyright   : Your copyright notice
// Description : Hello World in C, Ansi-style
//============================================================================

#include <stdio.h>
#include <stdlib.h>
#include <cassert>
#include <cstring>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
#include "storage_engine.h"
#include "heap_storage.h"

const char *EXAMPLE = "example.db";
const unsigned int BLOCK_SZ = 4096;
DbEnv* _DB_ENV;

void createDb(DbEnv *env, char *name) {
	Db db(env, 0);
	db.set_message_stream(env->get_message_stream());
	db.set_error_stream(env->get_error_stream());
	db.set_re_len(BLOCK_SZ); // Set record length to 4K
	db.open(nullptr, name, nullptr, DB_RECNO, DB_CREATE | DB_TRUNCATE, 0644); // Erases anything already there
}

void testDb(Db *db) {
	char block[BLOCK_SZ];
	Dbt data(block, sizeof(block));
	int block_number;
	Dbt key(&block_number, sizeof(block_number));
	block_number = 1;
	strcpy(block, "hello!");
	db->put(nullptr, &key, &data, 0);  // write block #1 to the database

	Dbt rdata;
	db->get(nullptr, &key, &rdata, 0); // read a block #1 from the database
	std::cout << "Read (block #" << block_number << "): '"
			<< (char *) rdata.get_data() << "'" << std::endl;

	assert(memcmp(block, rdata.get_data(), BLOCK_SZ) == 0);

	db->del(nullptr, &key, 0);
}

std::string expressionToString(const hsql::Expr*);

std::string operatorExpressionToString(const hsql::Expr* expr) {
	if (expr == NULL)
		return "null";

	std::string ret;
	if(expr->opType == hsql::Expr::NOT)
		ret += "NOT ";
	ret += expressionToString(expr->expr) + " ";
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
		ret += " " + expressionToString(expr->expr2);
	return ret;
}

std::string expressionToString(const hsql::Expr *expr) {
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
		ret += operatorExpressionToString(expr);
		break;
	default:
		ret += "???";
		break;
	}
	if (expr->alias != NULL)
		ret += std::string(" AS ") + expr->alias;
	return ret;
}

std::string tableRefInfoToString(const hsql::TableRef *table) {
	std::string ret;
	switch (table->type) {
	case hsql::kTableName:
		ret += table->name;
		if (table->alias != NULL)
			ret += std::string(" AS ") + table->alias;
		break;
	case hsql::kTableJoin:
		ret += tableRefInfoToString(table->join->left);
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
		ret += tableRefInfoToString(table->join->right);
		if (table->join->condition != NULL)
			ret += " ON " + expressionToString(table->join->condition);
		break;
	case hsql::kTableCrossProduct:
		bool doComma = false;
		for (hsql::TableRef* tbl : *table->list) {
			if (doComma)
				ret += ", ";
			ret += tableRefInfoToString(tbl);
			doComma = true;
		}
		break;
	}
	return ret;
}

std::string columnDefinitionToString(const hsql::ColumnDefinition *col) {
	std::string ret(col->name);
	switch(col->type) {
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

std::string executeSelect(const hsql::SelectStatement *stmt) {
	std::string ret("SELECT ");
	bool doComma = false;
	for (hsql::Expr* expr : *stmt->selectList) {
		if(doComma)
			ret += ", ";
		ret += expressionToString(expr);
		doComma = true;
	}
	ret += " FROM " + tableRefInfoToString(stmt->fromTable);
	if (stmt->whereClause != NULL)
		ret += " WHERE " + expressionToString(stmt->whereClause);
	return ret;
}

std::string executeInsert(const hsql::InsertStatement *stmt) {
	return "INSERT ...";
}

std::string executeCreate(const hsql::CreateStatement *stmt) {
	std::string ret("CREATE TABLE ");
	if (stmt->type != hsql::CreateStatement::kTable )
		return ret + "...";
	if (stmt->ifNotExists)
		ret += "IF NOT EXISTS ";
	ret += std::string(stmt->tableName) + " (";
	bool doComma = false;
	for (hsql::ColumnDefinition *col : *stmt->columns) {
		if(doComma)
			ret += ", ";
		ret += columnDefinitionToString(col);
		doComma = true;
	}
	ret += ")";
	return ret;
}

std::string execute(const hsql::SQLStatement *stmt) {
	switch (stmt->type()) {
	case hsql::kStmtSelect:
		return executeSelect((const hsql::SelectStatement*) stmt);
	case hsql::kStmtInsert:
		return executeInsert((const hsql::InsertStatement*) stmt);
	case hsql::kStmtCreate:
		return executeCreate((const hsql::CreateStatement*) stmt);
	default:
		return "Not implemented";
	}
}

int main(int argc, char *argv[]) {

	if (argc != 2) {
		std::cerr << "Usage: cpsc4300: dbenvpath" << std::endl;
		return 1;
	}
	char *envHome = argv[1];
	std::cout << "(sql4300: running with database environment at " << envHome
			<< ")" << std::endl;

	DbEnv env(0U);
	env.set_message_stream(&std::cout);
	env.set_error_stream(&std::cerr);
	try {
		env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
	} catch (DbException& exc) {
		std::cerr << "(sql4300: " << exc.what() << ")";
		exit(1);
	}
	_DB_ENV = &env;

	while (true) {
		std::cout << "SQL> ";
		std::string query;
		std::getline(std::cin, query);
		if (query.length() == 0)
			continue;
		if (query == "quit")
			break;
		if (query == "test") {
			std::cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << std::endl;
			continue;
		}
		hsql::SQLParserResult* result = hsql::SQLParser::parseSQLString(query);
		if (!result->isValid()) {
			std::cout << "invalid SQL: " << query << std::endl;
			continue;
		}
		for (uint i = 0; i < result->size(); ++i) {
			std::cout << execute(result->getStatement(i)) << std::endl;
		}
	}

	return EXIT_SUCCESS;
}
