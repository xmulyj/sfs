#!/bin/bash

### create test dir ###
TEST_DIR=/data
echo -e "create test dir:${TEST_DIR}...................[ \033[32mOK\033[0m ]"
if [ ! -d ${TEST_DIR} ];then
	sudo mkdir -p ${TEST_DIR}
fi
sudo chmod ugo+w ${TEST_DIR}

### crete test.txt ###
echo -e "create test file:${TEST_DIR}/test.txt.........[ \033[32mOK\033[0m ]"
string="hello,sfs!!!\nsfs:the Small File storage System.\n=======================================\n"
echo -e "$string" > ${TEST_DIR}/test.txt

### start master ###
echo -n "[1] start master server..."
cd master
./sfs_master &
cd ..
sleep 1
echo -e "..............[ \033[32mOK\033[0m ]"

### start chunk ###
echo -n "[2] start chunk server..."
cd chunk
./sfs_chunk &
cd ..
sleep 5
echo -e "...............[ \033[32mOK\033[0m ]"

### start client sfs ###
echo -e "[3] try to save file:${TEST_DIR}/test.txt.....[ \033[32mOK\033[0m ]\n\n"
cd client
./sfs
cd ..
echo -e "\n\n[4] save file status....................[ \033[32mOK\033[0m ]"


echo -e "[5] kill server.........................[ \033[32mOK\033[0m ]"
killall sfs_master  >/dev/null 1>&1 2>&1
killall sfs_chunk   >/dev/null 1>&1 2>&1

echo -e "[6] all tasks finished..................[ \033[32mDO\033[0m ]"
