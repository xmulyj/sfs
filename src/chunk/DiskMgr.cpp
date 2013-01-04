/*
 * DiskMgr.cpp
 *
 *  Created on: 2012-12-28
 *      Author: LiuYongJin
 */

#include "DiskMgr.h"
#include "slog.h"


#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <string.h>
#include <assert.h>
#include <stdio.h>
#include <sys/statfs.h>

#include "ConfigReader.h"
extern ConfigReader* g_config_reader;

#define LOCK(lock) pthread_mutex_lock(&lock)
#define UNLOCK(lock) pthread_mutex_unlock(&lock)

void DiskMgr::make_path(string &path, string &fid, int index)
{
	char buf[256];
	if(index >= 0)
		sprintf(buf, "%s/%c%c/%02X", m_disk_path.c_str(), fid[0], fid[1], index);
	else
		sprintf(buf, "%s/%c%c", m_disk_path.c_str(), fid[0], fid[1]);
	path = buf;
}

DiskMgr::DiskMgr()
{
	m_disk_path = g_config_reader->GetValueString("DiskDir", "/tmp/sfs_chunk");  //数据存放路径
	m_disk_space = 0L;
	m_disk_used  = 0L;
}

void DiskMgr::init()
{
	pthread_mutex_init(&m_disk_lock, NULL);
	load_disk(); //加载磁盘
	update();    //更新磁盘信息
}

void DiskMgr::uninit()
{
	pthread_mutex_destroy(&m_disk_lock);
	unload_disk();
}

void DiskMgr::load_disk()
{
	int i;
	char pre_fix[3];
	struct stat path_stat;

	//目录不存在
	if(stat(m_disk_path.c_str(), &path_stat)==-1 && errno==ENOENT)
	{
		int result = mkdir(m_disk_path.c_str(), S_IRWXU);
		assert(result == 0);
	}

	//加载00,01,...,FF  256个子目录
	for(i=0; i<DIR_NUM; ++i)
	{
		int index = 0;
		sprintf(pre_fix, "%02X", i);
		m_disk_files[i].pre_fix = pre_fix;
		m_disk_files[i].fp = NULL;
		m_disk_files[i].index = -1;
		m_disk_files[i].cur_pos = 0;
		pthread_mutex_init(&m_disk_files[i].lock, NULL);

		string sub_dir = m_disk_path+"/"+pre_fix;
		SLOG_DEBUG("checking chunk_dir:%s.", sub_dir.c_str());
		if(stat(sub_dir.c_str(), &path_stat)==-1 && errno==ENOENT) //子目录不存在
		{
			SLOG_INFO("sub dir[%s] not exists. create it.", sub_dir.c_str());
			if(mkdir(sub_dir.c_str(), S_IRWXU) == -1)
			{
				SLOG_ERROR("make dir failed.sub_dir=%s, error:%s", sub_dir.c_str(), strerror(errno));
				continue;
			}
		}
		else if(!S_ISDIR(path_stat.st_mode)) //不是目录
		{
			SLOG_ERROR("not dir.sub_dir=%s.", sub_dir.c_str());
			continue;
		}

		//open file
		struct dirent* ent = NULL;
		DIR *dir;
		if((dir=opendir(sub_dir.c_str())) == NULL)
		{
			SLOG_ERROR("open dir error. sub_dir=%s, error:%s", sub_dir.c_str(), strerror(errno));
			continue;
		}
		while((ent=readdir(dir)) != NULL)  //计算文件数
		{
			if(strcmp( ".",ent->d_name) == 0 || strcmp( "..",ent->d_name) == 0)
				continue;
			++index;
		}
		closedir(dir);
		if(index > 0)  //最后一个文件
			--index;

		char name[256];
		sprintf(name, "%s/%02X", sub_dir.c_str(), index);
		m_disk_files[i].fp= fopen(name, "a");
		assert(m_disk_files[i].fp != NULL);
		//fseek(m_disk_files[i].fp, 0L, SEEK_END);
		m_disk_files[i].cur_pos = (uint32_t)ftell(m_disk_files[i].fp);
		m_disk_files[i].index = index;

		//SLOG_DEBUG("load file succ. name=%s, size=%d.", name, m_disk_files[i].cur_pos);
	}
}
void DiskMgr::unload_disk()
{
	int i;
	for(i=0; i<DIR_NUM; ++i)
	{
		if(m_disk_files[i].fp != NULL)
			fclose(m_disk_files[i].fp);
		pthread_mutex_destroy(&m_disk_files[i].lock);
	}
}

bool DiskMgr::save_file_to_disk(string &fid, char *buf, uint32_t size, ChunkPath &chunkpath)
{
	const char *temp = fid.c_str();
	int i=0, index= 0;
	for(; i<2; ++i)  //get index
	{
		index *= 16;
		if(temp[i]>='A' && temp[i]<='F')
			index += temp[i]-'A'+10;
		else if(temp[i]>='a' && temp[i]<='f')
			index += temp[i]-'a'+10;
		else if(temp[i]>='0' && temp[i]<='9')
			index += temp[i]-'0';
		else
			return false;
	}
	assert(index>=0 && index<DIR_NUM);
	DiskFile &disk_file = m_disk_files[index];
	assert(disk_file.fp != NULL);

	LOCK(disk_file.lock);

	if(disk_file.cur_pos+size > MAX_FLESIZE)  //重新打开文件
	{
		fclose(disk_file.fp);
		string path;
		make_path(path, fid, ++disk_file.index);
		disk_file.fp = fopen(path.c_str(), "w");
		assert(disk_file.fp != NULL);
		disk_file.cur_pos = 0;
	}

	chunkpath.id = g_config_reader->GetValueString("ChunkID");
	assert(chunkpath.id != "");
	chunkpath.ip = g_config_reader->GetValueString("ChunkIP");
	assert(chunkpath.ip != "");
	chunkpath.port = g_config_reader->GetValueInt("ChunkPort", 3013);
	chunkpath.index = disk_file.index;
	chunkpath.offset = disk_file.cur_pos;

	fwrite(buf, 1, size, disk_file.fp);
	fflush(disk_file.fp);
	disk_file.cur_pos += size;

	UNLOCK(disk_file.lock);
	return true;
}

void DiskMgr::update()
{
	LOCK(m_disk_lock);

	SLOG_DEBUG("start update disk manager.");
	m_disk_space = 0L;
	m_disk_used  = 0L;
	struct statfs disk_statfs;
	if(statfs(m_disk_path.c_str(), &disk_statfs) >= 0)
	{
		m_disk_space = ((uint64_t)disk_statfs.f_bsize*(uint64_t)disk_statfs.f_blocks)>>10;               //KB
		m_disk_used  = m_disk_space - (((uint64_t)disk_statfs.f_bsize*(uint64_t)disk_statfs.f_bfree)>>10);  //KB
	}
	else
		SLOG_ERROR("statfs error. errno=%d(%s). set total_space=0, used_space=0.", errno, strerror(errno));

	UNLOCK(m_disk_lock);
}

void DiskMgr::get_disk_space(uint64_t &total, uint64_t &used)
{
	LOCK(m_disk_lock);

	total = m_disk_space;
	used = m_disk_used;

	UNLOCK(m_disk_lock);
}
