/*
 * DownloadServer.cpp
 *
 *  Created on: 2012-9-16
 *      Author: LiuYongjin
 */

#include "MasterServer.h"
#include "IODemuxerEpoll.h"

#include "slog.h"
#include <stdio.h>

#include "ConfigReader.h"
extern ConfigReader *g_config_reader;

/////////////////////////////////////// MasterServer ///////////////////////////////////////
bool MasterServer::start_server()
{
	//Init NetInterface
	init_net_interface();
	set_thread_ready();

	////Add your codes here
	///////////////////////
	m_saving_task_timeout_sec = g_config_reader->GetValueInt("SavingTaskTimeout", 120);

	//数据库
	m_db_connection = NULL;
	m_db_ip     = g_config_reader->GetValueString("DBIP");
	m_db_port   = g_config_reader->GetValueInt("DBPort", 0);
	m_db_user   = g_config_reader->GetValueString("DBUser");
	m_db_passwd = g_config_reader->GetValueString("DBPassword");
	m_db_name   = g_config_reader->GetValueString("DBName");
	if(m_db_ip!="" && m_db_user!="" && m_db_passwd!="" && m_db_name!="")
	{
		m_db_connection = new Connection(m_db_name.c_str(), m_db_ip.c_str(), m_db_user.c_str(), m_db_passwd.c_str(), m_db_port);
		if(!m_db_connection->connected())
		{
			SLOG_ERROR("connect DB error.db=%s, ip=%s, port=%d, user=%s, pwd=%s."
						,m_db_name.c_str()
						,m_db_ip.c_str()
						,m_db_port
						,m_db_user.c_str()
						,m_db_passwd.c_str());
			delete m_db_connection;
		}
	}
	else
	{
		SLOG_WARN("DB parameters is invalid. no using DB!!!");
	}

	//注册定时器
	IODemuxer *io_demuxer = get_io_demuxer();
	assert(io_demuxer != NULL);
	if(io_demuxer->register_event(-1, EVENT_PERSIST, 3000, this) == -1)
	{
		SLOG_ERROR("register timer handler failed.");
		return false;
	}
	io_demuxer->run_loop();
	return true;
}

HANDLE_RESULT MasterServer::on_timeout(int fd)
{
	//检查超时的任务
	SLOG_DEBUG("master on_timeout,check all task...");
	remove_saving_task_timeout();

	return HANDLE_OK;
}

ProtocolFamily* MasterServer::create_protocol_family()
{
	return new SFSProtocolFamily;
}

void MasterServer::delete_protocol_family(ProtocolFamily* protocol_family)
{
	delete protocol_family;
}

bool MasterServer::on_recv_protocol(SocketHandle socket_handle, Protocol *protocol, bool &detach_protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	DefaultProtocolHeader *header = (DefaultProtocolHeader *)protocol->get_protocol_header();
	switch(header->get_protocol_type())
	{
	case PROTOCOL_FILE_INFO_REQ:  //响应FileInfo请求
	{
		on_file_info_req(socket_handle, protocol);
		break;
	}
	case PROTOCOL_CHUNK_PING:    //响应chunk的ping包
	{
		on_chunk_ping(socket_handle, protocol);
		break;
	}
	case PROTOCOL_FILE_INFO:  //chunk 上报文件信息
	{
		on_file_info(socket_handle, protocol);
		break;
	}
	default:
		SLOG_WARN("Thread[ID=%d] fd=%d receive undefined protocol. ignore it.", get_thread_id(), socket_handle);
		return false;
	}

	return true;
}

bool MasterServer::on_protocol_send_error(SocketHandle socket_handle, Protocol *protocol)
{
	SLOG_ERROR("Thread[ID=%d] send protocol[details=%s] error. fd=%d, protocol=%x", get_thread_id(), protocol->details(), socket_handle, protocol);
	//Add your code to handle the protocol
	//////////////////////////////////////

	get_protocol_family()->destroy_protocol(protocol);
	return true;
}

bool MasterServer::on_protocol_send_succ(SocketHandle socket_handle, Protocol *protocol)
{
	SLOG_INFO("Thread[ID=%d] send protocol[details=%s] succ. fd=%d, protocol=%x", get_thread_id(), protocol->details(), socket_handle, protocol);
	//Add your code to handle the protocol
	//////////////////////////////////////

	get_protocol_family()->destroy_protocol(protocol);
	return true;
}

bool MasterServer::on_socket_handle_error(SocketHandle socket_handle)
{
	SLOG_INFO("Thread[ID=%d] handle socket error. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle the socket error
	//////////////////////////////////////////

	return true;
}

bool MasterServer::on_socket_handle_timeout(SocketHandle socket_handle)
{
	SLOG_INFO("Thread[ID=%d] handle socket timeout. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle the socket timeout
	////////////////////////////////////////////

	return true;
}

bool MasterServer::on_socket_handler_accpet(SocketHandle socket_handle)
{
	SLOG_DEBUG("Thread[ID=%d] handle new socket. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle new socket
	////////////////////////////////////

	return true;
}

///////////////////////////////////////////////////////////
void MasterServer::add_chunk(ChunkInfo &chunk_info)
{
	map<string, ChunkInfo>::iterator it = m_chunk_manager.find(chunk_info.id);
	if(it == m_chunk_manager.end())
	{
		m_chunk_manager.insert(std::make_pair(chunk_info.id, chunk_info));
		SLOG_DEBUG("add a new chunk info[chunk_id=%s, ip=%s, port=%d, disk_space=%lld, disk_used=%lld]. total chunk:%d."
					,chunk_info.id.c_str()
					,chunk_info.ip.c_str()
					,chunk_info.port
					,chunk_info.disk_space
					,chunk_info.disk_used
					,m_chunk_manager.size());
	}
	else
	{
		it->second = chunk_info;
		SLOG_DEBUG("update chunk info[chunk_id=%s, ip=%s, port=%d, disk_space=%lld, disk_used=%lld]. total chunk:%d."
					,chunk_info.id.c_str()
					,chunk_info.ip.c_str()
					,chunk_info.port
					,chunk_info.disk_space
					,chunk_info.disk_used
					,m_chunk_manager.size());
	}
}

bool MasterServer::get_chunk(ChunkInfo &chunk_info)
{
	map<string, ChunkInfo>::iterator it = m_chunk_manager.begin();
	if(it != m_chunk_manager.end())
	{
		chunk_info = it->second;
		return true;
	}

	return false;
}

bool MasterServer::get_fileinfo(const string &fid, FileInfo &fileinfo)
{
	//查找cache
	map<string, FileInfo>::iterator it = m_fileinfo_cache.find(fid);
	if(it != m_fileinfo_cache.end())
	{
		fileinfo = it->second;
		return true;
	}
	//查找数据库
	if(m_db_connection == NULL)
		return false;

	char sql_str[1024];
	snprintf(sql_str, 1024, "select fid,name,size,chunkid,chunkip,chunkport,findex,foffset from SFS.fileinfo_%s where fid='%s'"
							,fid.substr(0,2).c_str(), fid.c_str());
	Query query = m_db_connection->query(sql_str);
	StoreQueryResult res = query.store();
	if (!res || res.empty())
		return false;

	size_t i;
	for(i=0; i<res.num_rows(); ++i)
	{
		ChunkPath chunk_path;
		fileinfo.fid      = res[i]["fid"].c_str();
		fileinfo.name     = res[i]["name"].c_str();
		fileinfo.size     = atoi(res[i]["size"].c_str());
		chunk_path.id     = res[i]["chunkid"].c_str();
		chunk_path.ip     = res[i]["chunkip"].c_str();
		chunk_path.port   = atoi(res[i]["chunkport"].c_str());
		chunk_path.index  = atoi(res[i]["findex"].c_str());
		chunk_path.offset = atoi(res[i]["foffset"].c_str());

		fileinfo.add_chunkpath(chunk_path);
	}
	//添加到cache
	m_fileinfo_cache.insert(std::make_pair(fileinfo.fid, fileinfo));

	return true;
}

bool MasterServer::find_saving_task(const string &fid)
{
	return m_saving_task_map.find(fid) != m_saving_task_map.end();
}

bool MasterServer::add_saving_task(const string &fid)
{
	if(find_saving_task(fid))  //已经存在
	{
		SLOG_WARN("saving task[fd=%s] already exists.", fid.c_str());
		return true;
	}

	TimeFid time_fid;
	time_fid.insert_time = (int)time(NULL);
	time_fid.fid = fid;
	m_time_fid_list.push_front(time_fid);  //保存到list头
	m_saving_task_map.insert(std::make_pair(fid, m_time_fid_list.begin()));  //保存到map

	SLOG_INFO("add saving task:fid=%s insert_time=%d.", fid.c_str(), time_fid.insert_time);
	return true;
}

bool MasterServer::remove_saving_task(string &fid)
{
	map<string, list<TimeFid>::iterator>::iterator it = m_saving_task_map.find(fid);
	if(it == m_saving_task_map.end())  //不存在
		return false;
	m_time_fid_list.erase(it->second);
	m_saving_task_map.erase(it);
	return true;
}

bool MasterServer::remove_saving_task_timeout()
{
	int now = (int)time(NULL);
	list<TimeFid>::iterator it;
	while(m_time_fid_list.size() > 0)
	{
		it = m_time_fid_list.end();
		--it;
		if(now-it->insert_time < m_saving_task_timeout_sec)
			break;
		SLOG_DEBUG("saving task timeout and delete:fid=%s, instert_time=%d, now=%d.", it->fid.c_str(), it->insert_time, now);
		m_saving_task_map.erase(it->fid);
		m_time_fid_list.erase(it);
	}

	return true;
}

//保存到数据库
bool MasterServer::save_fileinfo_to_db(FileInfo &fileinfo)
{
	if(m_db_connection == NULL)
		return false;
	char sql_str[1024];
	ChunkPath &chunk_path = fileinfo.get_chunkpath(0);
	snprintf(sql_str, 1024, "insert into SFS.fileinfo_%s (fid, name, size, chunkid, chunkip, chunkport, findex, foffset) "
			"values('%s', '%s', %d, '%s', '%s', %d, %d, %d);"
			,fileinfo.fid.substr(0,2).c_str(), fileinfo.fid.c_str(), fileinfo.name.c_str(), fileinfo.size
			,chunk_path.id.c_str() ,chunk_path.ip.c_str(), chunk_path.port
			,chunk_path.index, chunk_path.offset);
	Query query = m_db_connection->query(sql_str);
	return query.exec();
}

//////////////////////////////////////////////////////////////
//响应chunk的ping包
void MasterServer::on_chunk_ping(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	ProtocolChunkPingResp *protocol_chunkping_resp = (ProtocolChunkPingResp *)protocol_family->create_protocol(PROTOCOL_CHUNK_PING_RESP);
	assert(protocol_chunkping_resp);

	ProtocolChunkPing *protocol_chunkping = (ProtocolChunkPing *)protocol;
	ChunkInfo &chunk_info = protocol_chunkping->get_chunk_info();
	SLOG_INFO("receive chunk_ping protocol.fd=%d, chunk_id=%s, chunk_addr=%s, chunk_port=%d, disk_space=%lld, disk_used=%lld."
				,socket_handle
				,chunk_info.id.c_str()
				,chunk_info.ip.c_str()
				,chunk_info.port
				,chunk_info.disk_space
				,chunk_info.disk_used);

	if(chunk_info.id == "" || chunk_info.ip == "")
	{
		SLOG_ERROR("chunk id or ip is empty.");
		protocol_chunkping_resp->get_result() = false;
	}
	else
	{
		add_chunk(chunk_info);
		protocol_chunkping_resp->get_result() = true;
	}

	if(!send_protocol(socket_handle, protocol_chunkping_resp))
	{
		protocol_family->destroy_protocol(protocol_chunkping_resp);
		SLOG_ERROR("send protocol_chunkping_resp failed.fd=%d, chunk_id=%s.", socket_handle, chunk_info.id.c_str());
	}
}

//响应文件信息查询包
void MasterServer::on_file_info_req(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	ProtocolFileInfo *protocol_fileinfo = (ProtocolFileInfo *)protocol_family->create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_fileinfo != NULL);

	ProtocolFileInfoReq *protocol_file_info_req = (ProtocolFileInfoReq *)protocol;
	const string& fid = protocol_file_info_req->get_fid();
	bool query_chunkpath = protocol_file_info_req->get_query_chunkpath();
	SLOG_INFO("receive file_info_req protocol.fd=%d, fid=%s, query=%d", socket_handle, fid.c_str(), query_chunkpath?1:0);

	FileInfo& file_info = protocol_fileinfo->get_fileinfo();
	if(get_fileinfo(fid, file_info))  //已经存在
	{
		SLOG_DEBUG("find file_info succ: fid=%s, size=%d.", fid.c_str(), file_info.size);
		int i;
		for(i=0; i<file_info.get_chunkpath_count(); ++i)
		{
			ChunkPath &chunk_path = file_info.get_chunkpath(i);
			SLOG_DEBUG("chunk[%d]:id=%s, ip=%s, port=%d, index=%d, offset=%d."
					,i, chunk_path.id.c_str(), chunk_path.ip.c_str(), chunk_path.port, chunk_path.index, chunk_path.offset);
		}
		file_info.result = FileInfo::RESULT_SUCC;
	}
	else if(find_saving_task(fid))  //正在保存
	{
		SLOG_DEBUG("fid=%s is saving.", fid.c_str());
		file_info.result = FileInfo::RESULT_SAVING;
	}
	else if(query_chunkpath)  //分配chunk
	{
		file_info.fid = fid;
		file_info.name = ""; //无效
		file_info.size = 0;  //无效

		ChunkPath chunk_path;
		ChunkInfo chunk_info;
		if(get_chunk(chunk_info))  //分配chunk
		{
			file_info.result = FileInfo::RESULT_CHUNK;
			chunk_path.id = chunk_info.id;
			chunk_path.ip = chunk_info.ip;
			chunk_path.port = chunk_info.port;
			chunk_path.index = 0;  //无效
			chunk_path.offset = 0; //无效
			file_info.add_chunkpath(chunk_path);

			add_saving_task(fid);
			SLOG_DEBUG("dispatch chunk[id=%s,ip=%s,port=%d] for fid=%s.", chunk_info.id.c_str(), chunk_info.ip.c_str(), chunk_info.port, fid.c_str());
		}
		else
		{
			SLOG_WARN("get chunk failed for fid=%s.", fid.c_str());
			file_info.result = FileInfo::RESULT_FAILED;
		}
	}
	else  //失败
	{
		SLOG_WARN("get file_info failed for fid=%s.", fid.c_str());
		file_info.result = FileInfo::RESULT_FAILED;
	}

	if(!send_protocol(socket_handle, protocol_fileinfo))
	{
		SLOG_ERROR("send file_info protocol failed. fd=%d, fid=%s.", socket_handle, fid.c_str());
		protocol_family->destroy_protocol(protocol_fileinfo);
	}
}

//响应chunk发送file info保存包
void MasterServer::on_file_info(SocketHandle socket_handle, Protocol *protocol)
{
	ProtocolFileInfo *protocol_fileinfo = (ProtocolFileInfo *)protocol;
	FileInfo &fileinfo = protocol_fileinfo->get_fileinfo();
	SLOG_INFO("receive file_info protocol. fd=%d, result=%d, fid=%s.", socket_handle, (int)fileinfo.result, fileinfo.fid.c_str());

	if(fileinfo.result != FileInfo::RESULT_SUCC)  //chunk保存失败,不需要回复
	{
		SLOG_INFO("chunk save file failed, remove saving task. fid=%s.", fileinfo.fid.c_str());
		remove_saving_task(fileinfo.fid);
		return ;
	}

	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	ProtocolFileInfoSaveResult *protocol_fileinfo_save_result = (ProtocolFileInfoSaveResult *)protocol_family->create_protocol(PROTOCOL_FILE_INFO_SAVE_RESULT);
	assert(protocol_fileinfo_save_result != NULL);
	FileInfoSaveResult &save_result = protocol_fileinfo_save_result->get_save_result();
	save_result.fid = fileinfo.fid;

	if(find_saving_task(fileinfo.fid)) //找到正在保存任务
	{
		ChunkPath &chunk_path = fileinfo.get_chunkpath(0);
		SLOG_INFO("save file info succ: fid=%s, name=%s, size=%d, chunkid=%s, addr=%s, port=%d, index=%d, offset=%d."
					,fileinfo.fid.c_str()
					,fileinfo.name.c_str()
					,fileinfo.size
					,chunk_path.id.c_str()
					,chunk_path.ip.c_str()
					,chunk_path.port
					,chunk_path.index
					,chunk_path.offset);

		m_fileinfo_cache.insert(std::make_pair(fileinfo.fid, fileinfo));
		save_fileinfo_to_db(fileinfo);
		remove_saving_task(fileinfo.fid);

		save_result.result = FileInfoSaveResult::RESULT_SUCC;
	}
	else //找不到正在保存的任务
	{
		SLOG_WARN("can't find saving task. fd=%d, fid=%s.", socket_handle, fileinfo.fid.c_str());
		save_result.result = FileInfoSaveResult::RESULT_FAILED;
	}

	if(!send_protocol(socket_handle, protocol_fileinfo_save_result))
	{
		protocol_family->destroy_protocol(protocol_fileinfo_save_result);
		SLOG_ERROR("send fileinfo_save_result protocol failed. fd=%d, fid=%s", socket_handle, fileinfo.fid.c_str());
	}
}

/////////////////////////////////////// MasterThreadPool ///////////////////////////////////////
Thread<SocketHandle>* MasterThreadPool::create_thread()
{
	MasterServer* temp = new MasterServer();
	temp->set_idle_timeout(30000);
	return (Thread<SocketHandle>*)temp;
}

