INCS=-I/usr/local/include/libnetframework -I../
LIBS=-lslog -lnetframework -lpthread -lssl
FLAGS=-g
DEPS=../CommonType.h ../SFSProtocolFamily.h ../SFSProtocolFamily.cpp

all:sfs_chunk

sfs_chunk:*.h *.cpp ${DEPS}
	g++ ${INCS} $^ ${LIBS} ${FLAGS} -o $@
clean:
	rm sfs_chunk