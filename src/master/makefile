INCS=-I/usr/local/include/libnetframework -I../ 
LIBS=-lslog -lnetframework -lpthread -lssl
FLAGS=-g
DEPS=../SFSProtocolFamily.h ../SFSProtocolFamily.cpp

all:sfs_master

sfs_master:*.h *.cpp ${DEPS}
	g++ ${INCS} $^ ${LIBS} ${FLAGS} -o $@
clean:
	rm sfs_master
