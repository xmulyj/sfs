/*
 * ServerAppFramework.h
 *
 *  Created on: 2012-11-09
 *      Author: LiuYongJin
 */

#ifndef APP_SFS_CHUNK_WORKER_H_20121109
#define APP_SFS_CHUNK_WORKER_H_20121109

#include "ConnectThread.h"
#include "ConnectThreadPool.h"

#include "SFSProtocolFamily.h"

#include <stdint.h>
#include <map>
#include <string>
using std::map;
using std::string;

#define DIR_NUM 256
#define MAX_FLESIZE 1024*1024*1024  //1G

typedef struct _file_task_
{
	SocketHandle socket_handle;
	string fid;
	string name;
	uint64_t size;
	void *buf;
	FileInfo file_info;
}FileTask;
typedef map<string, FileTask> FileTaskMap;  //fid-filetask

typedef struct _disk_file
{
	string pre_fix;
	FILE *fp;
	int index;
	uint64_t cur_pos;
	pthread_mutex_t lock;
}DiskFile;

class ChunkWorker:public ConnectThread
{
protected:
	////由应用层实现 -- 创建具体的协议族
	virtual ProtocolFamily* create_protocol_family();
	////由应用层实现 -- 销毁协议族
	virtual void delete_protocol_family(ProtocolFamily* protocol_family);

	////由应用层实现 -- 接收协议函数
	bool on_recv_protocol(SocketHandle socket_handle, Protocol *protocol, bool &detach_protocol);
	////由应用层实现 -- 协议发送错误处理函数
	bool on_protocol_send_error(SocketHandle socket_handle, Protocol *protocol);
	////由应用层实现 -- 协议发送成功处理函数
	bool on_protocol_send_succ(SocketHandle socket_handle, Protocol *protocol);
	////由应用层实现 -- 连接错误处理函数
	bool on_socket_handle_error(SocketHandle socket_handle);
	////由应用层实现 -- 连接超时处理函数
	bool on_socket_handle_timeout(SocketHandle socket_handle);
	////由应用层实现 -- 已经收到一个新的连接请求
	virtual bool on_socket_handler_accpet(SocketHandle socket_handle);
public:
	////由应用层实现 -- net interface实例启动入口
	bool start_server();

//////////////////// file task ////////////////////
private:
	pthread_mutex_t m_filetask_lock;
	FileTaskMap m_filetask_map;
	//查找文件任务
	bool file_task_find(string &fid);
	//创建一个文件任务
	bool file_task_create(SocketHandle socket_handle, FileSeg &file_seg);
	//删除一个文件任务
	void file_task_delete(string &fid);
	//保存文件分片数据
	bool file_task_save(FileSeg &file_seg);
	//文件已经传送完毕,保存到系统中
	bool save_file(string &fid);

//////////////////// disk file ////////////////////
private:
	string m_disk_path;
	DiskFile m_disk_files[DIR_NUM];
	//初始化,加载磁盘文件
	void load_disk_files();
//////////////////// 响应函数 /////////////////////
private:
	//响应客户端发送文件数据包
	void on_file(SocketHandle socket_handle, Protocol *protocol);
	//响应master回复file_info保存结果
	void on_file_info_save_result(SocketHandle socket_handle, Protocol *protocol);
};

class ChunkWorkerPool:public ConnectThreadPool
{
public:
	ChunkWorkerPool(unsigned int thread_num):ConnectThreadPool(thread_num){}
protected:
	//实现创建一个线程
	Thread<SocketHandle>* create_thread();
};

class TimerHandler:public EventHandler
{
public:
	TimerHandler(IODemuxer *demuxer):m_demuxer(demuxer){;}
	HANDLE_RESULT on_timeout(int fd);
private:
	IODemuxer *m_demuxer;
};

#endif //APP_SFS_CHUNK_WORKER_H_20121109

