#!/bin/bash

### create test dir ###
TEST_DIR=/data
echo "create test dir:${TEST_DIR}..."
if [ ! -d ${TEST_DIR} ];then
	sudo mkdir -p ${TEST_DIR}
fi
sudo chmod ugo+w ${TEST_DIR}

### crete test.txt ###
echo "create ${TEST_DIR}/test.txt"
string="hello,sfs!!!\nsfs:the Small File storage System.\n=======================================\n"
echo -e "$string" > ${TEST_DIR}/test.txt

### start master ###
echo "start master server"
cd master
./sfs_master &
cd ..
sleep 1

### start chunk ###
echo "start chunk server"
cd chunk
./sfs_chunk &
cd ..
sleep 5

### start client sfs ###
echo "run sfs to save file:${TEST_DIR}/test.txt"
cd client
./sfs
cd ..

killall sfs_master  >/dev/null 1>&1 2>&1
killall sfs_chunk   >/dev/null 1>&1 2>&1
