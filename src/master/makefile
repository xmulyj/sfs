INCS=-I/usr/local/include/libnetframework -I/usr/include/mysql -I/usr/include/mysql++ -I../ 
LIBS=-lslog -lnetframework -lpthread -L/usr/lib/mysql/ -lmysqlclient -lmysqlpp
FLAGS=-g
DEPS=../CommonType.h ../SFSProtocolFamily.h ../SFSProtocolFamily.cpp

all:sfs_master

sfs_master:*.h *.cpp ${DEPS}
	g++ ${INCS} $^ ${LIBS} ${FLAGS} -o $@
clean:
	rm sfs_master
