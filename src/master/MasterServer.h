/*
 * DownloadServer.h
 *
 *  Created on: 2012-9-16
 *      Author: LiuYongjin
 */

#ifndef APP_SFS_MASTER_SERVER_H_
#define APP_SFS_MASTER_SERVER_H_

#include "ConnectThread.h"
#include "ConnectThreadPool.h"

#include "SFSProtocolFamily.h"
#include "CommonType.h"

#include <stdint.h>
#include <string>
#include <map>
#include <list>
using namespace std;

class TimeFid
{
public:
	int insert_time;
	string fid;
};


class MasterServer:public ConnectThread
{
protected:  //重写EventHander的超时方法
	HANDLE_RESULT on_timeout(int fd); //定时时钟
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

private:
	//chunk信息管理
	map<string, ChunkInfo> m_chunk_manager;
	void add_chunk(ChunkInfo &chunk_ping);
	bool get_chunk(ChunkInfo &chunk_info);

	//文件信息cache
	map<string, FileInfo> m_fileinfo_cache;
	bool get_fileinfo(const string &fid, FileInfo &fileinfo);

	//正在存储的记录
	int m_saving_task_timeout_sec;   //超时时间
	map<string, list<TimeFid>::iterator> m_saving_task_map;
	list<TimeFid> m_time_fid_list;
	bool find_saving_task(const string &fid);
	bool add_saving_task(const string &fid);
	bool remove_saving_task(string &fid);
	bool remove_saving_task_timeout();
private:
	//响应chunk的ping包
	void on_chunk_ping(SocketHandle socket_handle, Protocol *protocol);
	//响应文件信息查询包
	void on_file_info_req(SocketHandle socket_handle, Protocol *protocol);
	//响应chunk发送fileinfo保存包
	void on_file_info(SocketHandle socket_handle, Protocol *protocol);
};

class MasterThreadPool:public ConnectThreadPool
{
public:
	MasterThreadPool(unsigned int thread_num):ConnectThreadPool(thread_num){}
protected:
	//实现创建一个线程
	Thread<SocketHandle>* create_thread();
};

#endif //APP_SERVER_DOWNLOAD_H_


