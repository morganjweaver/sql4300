CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c

%.o: %.cpp
	g++ -I/usr/local/db6/include -I/home/fac/lundeenk/sql-parser/src $(CCFLAGS) -o "$@" "$<"

cpsc4300a: cpsc4300a.o
	g++ -L/usr/local/db6/lib -L/home/fac/lundeenk/sql-parser -o $@ $< -ldb_cxx -lsqlparser
