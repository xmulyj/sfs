/*
 * DiskMgr.h
 *
 *  Created on: 2012-12-28
 *      Author: LiuYongJin
 */

#ifndef _DISK_MANAGER_H_
#define _DISK_MANAGER_H_

#include <stdint.h>
#include <pthread.h>
#include <string>
using std::string;

#include "CommonType.h"

#define DIR_NUM 256
#define MAX_FLESIZE 1024*1024*1024  //1G

typedef struct _disk_file
{
	string pre_fix;
	FILE *fp;
	int index;
	uint32_t cur_pos;
	pthread_mutex_t lock;
}DiskFile;

class DiskMgr
{
public:
	static DiskMgr* get_instance()
	{
		static DiskMgr* g_disk_mgr = NULL;
		if(g_disk_mgr == NULL)
			g_disk_mgr = new DiskMgr;
		return g_disk_mgr;
	}

	//初始化,加载磁盘文件
	void init();
	void uninit();

	//更新磁盘信息
	void update();
	//获取磁盘空间
	void get_disk_space(uint64_t &total, uint64_t &used);
	//将fid的size字节的数据buf保存到磁盘,返回chunk_path;成功返回true,失败返回false
	bool save_file_to_disk(string &fid, char *buf, uint32_t size, ChunkPath &chunkpath);
private:
	DiskMgr();
	void make_path(string &path, string &fid, int index);
	void load_disk();
	void unload_disk();
private:
	string m_disk_path;
	DiskFile m_disk_files[DIR_NUM];

	pthread_mutex_t m_disk_lock; //磁盘锁
	uint64_t m_disk_space;     //磁盘空间
	uint64_t m_disk_used;      //已用磁盘空间
};

#endif //_DISK_MANAGER_H_


