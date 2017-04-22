cmake_minimum_required(VERSION 3.7)
project(sql4300)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES
        heap_storage.cpp
        heap_storage.h
        sql4300.cpp
        storage_engine.h
        ParseTreeToString.cpp
        ParseTreeToString.h
        SQLExec.cpp SQLExec.h
        schema_tables.h schema_tables.cpp
        storage_engine.cpp)

#include_directories(/usr/local/db6/include)
include_directories(~/sql-parser/src)

add_executable(sql4300 ${SOURCE_FILES} Table.h Table.cpp)

#find_library(BERKELEY_DB db_cxx HINTS "/usr/local/db6/lib")
#target_link_libraries(sql4300 ${BERKELEY_DB})

find_library(SQL_PARSER SQLParser HINTS "~/sql-parser")
target_link_libraries(sql4300 ${SQL_PARSER})

link_directories(~/Documents/4300_DB/sql-parser)
link_libraries(sqlparser)
include_directories(~/Documents/4300_DB/sql-parser/src)
link_libraries(db_cxx) #libname
link_directories(/usr/local/BerkeleyDB.6.2/lib)  #//where to find libs
find_library(BERKELEY_DB db_cxx HINTS "/usr/local/BerkeleyDB.6.2/lib")
target_link_libraries(sql4300 ${BERKELEY_DB})
include_directories(/usr/local/BerkeleyDB.6.2/include) #where to find header files
add_executable(sql_interpreter ${SOURCE_FILES})
