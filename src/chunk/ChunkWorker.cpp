/*
 * MTServerAppFramework.cpp
 *
 *  Created on: 2012-9-11
 *      Author: xl
 */

#include "ChunkWorker.h"
#include "IODemuxerEpoll.h"
#include "SFSProtocolFamily.h"
#include "slog.h"
#include "DiskMgr.h"

#include "ConfigReader.h"
extern ConfigReader* g_config_reader;

///////////////////////////////  ChunkWorker  //////////////////////////////////
bool ChunkWorker::start_server()
{
	//Init NetInterface
	init_net_interface();
	set_thread_ready();

	////Add your codes here
	///////////////////////

	//pthread_mutex_init(&m_filetask_lock, NULL);
	m_master_ip = g_config_reader->GetValueString("MasterIP");
	assert(m_master_ip != "");
	m_master_port = g_config_reader->GetValueInt("MasterPort", 3012);

	m_master_socket_handle = SOCKET_INVALID;
	get_io_demuxer()->run_loop();
	return true;
}

ProtocolFamily* ChunkWorker::create_protocol_family()
{
	return new SFSProtocolFamily;
}

void ChunkWorker::delete_protocol_family(ProtocolFamily* protocol_family)
{
	delete protocol_family;
}

bool ChunkWorker::on_recv_protocol(SocketHandle socket_handle, Protocol *protocol, bool &detach_protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	DefaultProtocolHeader *header = (DefaultProtocolHeader *)protocol->get_protocol_header();
	switch(header->get_protocol_type())
	{
	case PROTOCOL_FILE:    //client 请求存储文件
	{
		on_file(socket_handle, protocol);
		break;
	}
	case PROTOCOL_FILE_INFO_SAVE_RESULT:    //master回复保存文件信息结果
	{
		on_file_info_save_result(socket_handle, protocol);
		break;
	}
	case PROTOCOL_FILE_REQ:
	{
		on_file_req(socket_handle, protocol);
		break;
	}
	default:
		SLOG_WARN("receive undefine protocol. ignore it.");
		return false;
	}

	return true;
}

bool ChunkWorker::on_protocol_send_error(SocketHandle socket_handle, Protocol *protocol)
{
	SLOG_ERROR("Thread[ID=%d] send protocol[details=%s] error. fd=%d, protocol=%x", get_thread_id(), protocol->details(), socket_handle, protocol);
	//Add your code to handle the protocol
	//////////////////////////////////////

	get_protocol_family()->destroy_protocol(protocol);
	return true;
}

bool ChunkWorker::on_protocol_send_succ(SocketHandle socket_handle, Protocol *protocol)
{
	SLOG_INFO("Thread[ID=%d] send protocol[details=%s] succ. fd=%d, protocol=%x", get_thread_id(), protocol->details(), socket_handle, protocol);
	//Add your code to handle the protocol
	//////////////////////////////////////

	get_protocol_family()->destroy_protocol(protocol);
	return true;
}

bool ChunkWorker::on_socket_handle_error(SocketHandle socket_handle)
{
	SLOG_INFO("Thread[ID=%d] handle socket error. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle the socket error
	//////////////////////////////////////////

	//m_master_socket_handle = get_active_trans_socket("127.0.0.1", 3012);  //创建主动连接到master
	m_master_socket_handle = SOCKET_INVALID;

	return true;
}

bool ChunkWorker::on_socket_handle_timeout(SocketHandle socket_handle)
{
	SLOG_INFO("Thread[ID=%d] handle socket timeout. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle the socket timeout
	////////////////////////////////////////////

	return true;
}

bool ChunkWorker::on_socket_handler_accpet(SocketHandle socket_handle)
{
	SLOG_DEBUG("Thread[ID=%d] handle new socket. fd=%d", get_thread_id(), socket_handle);
	//Add your code to handle new socket
	////////////////////////////////////

	return true;
}

///////////////////////////////////////////////////////////
SocketHandle ChunkWorker::get_master_connect()
{
	if(m_master_socket_handle == SOCKET_INVALID)
		m_master_socket_handle = get_active_trans_socket(m_master_ip.c_str(), m_master_port);  //创建主动连接到master
	assert(m_master_socket_handle != SOCKET_INVALID);
	return m_master_socket_handle;
}

void ChunkWorker::send_fail_fileinfo_to_master(string &fid)
{
	SFSProtocolFamily *protocol_family = (SFSProtocolFamily*)get_protocol_family();
	ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo *)protocol_family->create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_file_info != NULL);

	FileInfo &file_info = protocol_file_info->get_fileinfo();
	file_info.result = FileInfo::RESULT_FAILED;
	file_info.fid = fid;

	if(!send_protocol(get_master_connect(), protocol_file_info))
	{
		protocol_family->destroy_protocol(protocol_file_info);
		SLOG_ERROR("send faile_file_info to master failed. fid=%s.", fid.c_str());
	}
}

bool ChunkWorker::send_file_protocol_to_client(SocketHandle socket_handle, ProtocolFile *protocol_file, ByteBuffer *byte_buffer, int fd)
{
	return true;
}

bool ChunkWorker::file_task_find(string &fid)
{
	bool find;
	//pthread_mutex_lock(&m_filetask_lock);
	find = m_filetask_map.find(fid)!=m_filetask_map.end();
	//pthread_mutex_unlock(&m_filetask_lock);
	return find;
}

//创建一个文件任务
bool ChunkWorker::file_task_create(SocketHandle socket_handle, FileSeg &file_seg)
{
	bool result = false;
	//pthread_mutex_lock(&m_filetask_lock);

	FileTaskMap::iterator it = m_filetask_map.find(file_seg.fid);
	if(it == m_filetask_map.end())
	{
		FileTask file_task;
		file_task.socket_handle = socket_handle;
		file_task.fid = file_seg.fid;
		file_task.name = file_seg.name;
		file_task.size = file_seg.filesize;
		file_task.buf = (char*)malloc(file_seg.filesize);
		if(file_task.buf == NULL)
		{
			SLOG_ERROR("create file task failed:no memory.fid=%s, file_name=%s, file_size=%d, seg_size=%d."
						, file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize, file_seg.size);
		}
		else
		{
			result = true;
			m_filetask_map.insert(std::make_pair(file_task.fid, file_task));  //保存任务
			SLOG_DEBUG("create file task succ.fid=%s, file_name=%s, file_size=%d, seg_size=%d."
						, file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize, file_seg.size);
		}
	}
	else
		SLOG_WARN("file task already exists. fid=%s.", file_seg.fid.c_str());

	//pthread_mutex_unlock(&m_filetask_lock);
	return result;
}

//删除一个文件任务
void ChunkWorker::file_task_delete(string &fid)
{
	//pthread_mutex_lock(&m_filetask_lock);
	FileTaskMap::iterator it = m_filetask_map.find(fid);
	if(it != m_filetask_map.end())
	{
		FileTask &file_task = it->second;
		SLOG_DEBUG("delete file task succ:fid=%s, name=%s, size=%d", file_task.fid.c_str(), file_task.name.c_str(), file_task.size);
		free(file_task.buf);
		m_filetask_map.erase(it);
	}
	else
		SLOG_WARN("delete file task failed:can't find task[fid=%s].", fid.c_str());

	//pthread_mutex_unlock(&m_filetask_lock);
}

bool ChunkWorker::file_task_save(FileSeg &file_seg)
{
	bool result = false;
	//pthread_mutex_lock(&m_filetask_lock);

	FileTaskMap::iterator it = m_filetask_map.find(file_seg.fid);
	if(it != m_filetask_map.end())
	{
		FileTask &file_task = it->second;
		SLOG_DEBUG("file_task:fid=%s, size=%d. file_seg:total_size=%d, offset=%d, size=%d."
					,file_task.fid.c_str(), file_task.size, file_seg.filesize, file_seg.offset, file_seg.size);
		if(file_seg.offset+file_seg.size <= file_task.size)
		{
			result = true;
			memcpy(file_task.buf+file_seg.offset, file_seg.data, file_seg.size);
		}
	}
	else
		SLOG_WARN("can't find file task:fid=%s.", file_seg.fid.c_str());

	//pthread_mutex_unlock(&m_filetask_lock);
	return result;
}

bool ChunkWorker::save_file(string &fid)
{
	//pthread_mutex_lock(&m_filetask_lock);
	bool result = false;
	FileTaskMap::iterator it = m_filetask_map.find(fid);
	if(it != m_filetask_map.end())
	{
		//向master上报file_info
		SFSProtocolFamily *protocol_family = (SFSProtocolFamily*)get_protocol_family();
		ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo *)protocol_family->create_protocol(PROTOCOL_FILE_INFO);
		assert(protocol_file_info != NULL);

		FileTask &file_task = it->second;
		FileInfo &file_info = file_task.file_info;
		file_info.fid = fid;
		file_info.name = file_task.name;
		file_info.size = file_task.size;

		ChunkPath chunk_path;
		//保存到磁盘
		result = DiskMgr::get_instance()->save_file_to_disk(fid, file_task.buf, file_task.size, chunk_path);
		if(result != false)
		{
			file_info.add_chunkpath(chunk_path);
			file_info.result = FileInfo::RESULT_SUCC;
		}
		else
		{
			SLOG_ERROR("save fid=%s to file failed.", fid.c_str());
			file_info.result = FileInfo::RESULT_FAILED;
		}
		protocol_file_info->get_fileinfo() = file_info;

		if(!send_protocol(get_master_connect(), protocol_file_info))
		{
			result = false;
			protocol_family->destroy_protocol(protocol_file_info);
			SLOG_ERROR("send file info to master failed. fid=%s.", fid.c_str());
		}
	}
	else
		SLOG_WARN("save file task failed:can't find task[fid=%s].", fid.c_str());

	//pthread_mutex_unlock(&m_filetask_lock);

	return result;
}

///////////////////////////////////////////////////////////
//响应客户端发送文件数据包
void ChunkWorker::on_file(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	Protocol* protocol_resp = NULL;

	ProtocolFile *protocol_file = (ProtocolFile *)protocol;
	FileSeg &file_seg = protocol_file->get_file_seg();
	SLOG_INFO("receive File Protocol[file: flag=%d, fid=%s, name=%s, filesize=%d] [seg info: offset=%d, index=%d, size=%d]."
				,file_seg.flag, file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize, file_seg.offset, file_seg.index, file_seg.size);

	switch(file_seg.flag)
	{
		case FileSeg::FLAG_START:  //请求开始传输文件
		{
			protocol_resp = protocol_family->create_protocol(PROTOCOL_FILE_SAVE_RESULT);
			assert(protocol_resp != NULL);

			ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)protocol_resp;
			FileSaveResult &save_result = protocol_save_result->get_save_result();
			save_result.fid = file_seg.fid;
			if(file_task_find(file_seg.fid))  //已经存在
			{
				SLOG_WARN("file task already started. fid=%s.", file_seg.fid.c_str());
				save_result.status = FileSaveResult::CREATE_FAILED;
			}
			else if(!file_task_create(socket_handle, file_seg))  //创建任务失败
			{
				SLOG_ERROR("create file task failed. fid=%s.", file_seg.fid.c_str());
				save_result.status = FileSaveResult::CREATE_FAILED;
				//向master上报保存失败
				send_fail_fileinfo_to_master(file_seg.fid);
			}
			else  //成功
			{
				SLOG_INFO("create file task succ. fid=%s, name=%s, size=%d.", file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize);
				save_result.status = FileSaveResult::CREATE_SUCC;
			}
			break;
		}
		case FileSeg::FLAG_SEG:  //文件分片
		{
			protocol_resp = protocol_family->create_protocol(PROTOCOL_FILE_SAVE_RESULT);
			assert(protocol_resp != NULL);

			ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)protocol_resp;
			FileSaveResult &save_result = protocol_save_result->get_save_result();
			save_result.fid = file_seg.fid;
			save_result.status = FileSaveResult::SEG_SUCC;
			if(!file_task_save(file_seg))  //失败
			{
				SLOG_ERROR("save file seg failed. fid=%s.", file_seg.fid.c_str());
				save_result.status = FileSaveResult::SEG_FAILED;
				//向master上报保存失败
				send_fail_fileinfo_to_master(file_seg.fid);
				//删除正在保存的任务
				file_task_delete(file_seg.fid);
			}
			break;
		}
		case FileSeg::FLAG_END:  //已经结束
		{
			SLOG_INFO("client send file finished. fid=%s.", file_seg.fid.c_str());
			if(save_file(file_seg.fid)) //保存成功,等待master回复保存结果
				return ;

			//保存失败
			SLOG_ERROR("save file failed. fid=%s.", file_seg.fid.c_str());
			//删除正在保存的任务
			file_task_delete(file_seg.fid);

			//回复客户端失file info失败
			protocol_resp = protocol_family->create_protocol(PROTOCOL_FILE_INFO);
			assert(protocol_resp != NULL);
			ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo*)protocol_resp;
			FileInfo &file_info = protocol_file_info->get_fileinfo();
			file_info.result = FileInfo::RESULT_FAILED;
			file_info.fid = file_seg.fid;
			break;
		}
	}//switch

	if(!send_protocol(socket_handle, protocol_resp))
	{
		SLOG_ERROR("send file status protocol failed. fd=%d, fid=%s.", socket_handle, file_seg.fid.c_str());
		protocol_family->destroy_protocol(protocol_resp);
		file_task_delete(file_seg.fid);
	}
}

//响应master回复file_info保存结果
void ChunkWorker::on_file_info_save_result(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();

	ProtocolFileInfoSaveResult *protocol_save_result = (ProtocolFileInfoSaveResult*)protocol;
	FileInfoSaveResult &save_result = protocol_save_result->get_save_result();
	SLOG_INFO("fid=%s, save_result=%d.", save_result.fid.c_str(), save_result.result);

	//pthread_mutex_lock(&m_filetask_lock);
	FileTaskMap::iterator it = m_filetask_map.find(save_result.fid);
	if(it != m_filetask_map.end())
	{
		FileTask &file_task = it->second;
		ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo*)protocol_family->create_protocol(PROTOCOL_FILE_INFO);
		FileInfo &file_info = protocol_file_info->get_fileinfo();
		file_info = file_task.file_info;

		if(save_result.result == FileInfoSaveResult::RESULT_SUCC)  //master保存成功
		{
			file_info.result = FileInfo::RESULT_SUCC;
			ChunkPath &chunk_path = file_info.get_chunkpath(0);
			SLOG_DEBUG("chunk[%d]:id=%s, ip=%s, port=%d, index=%d, offset=%d."
						,0, chunk_path.id.c_str(), chunk_path.ip.c_str(), chunk_path.port, chunk_path.index, chunk_path.offset);
		}
		else
		{
			file_info.result = FileInfo::RESULT_FAILED;
			SLOG_WARN("master save file info failed. fid=%s.", save_result.fid.c_str());
		}

		if(!send_protocol(file_task.socket_handle, protocol_file_info))
		{
			SLOG_ERROR("send file info to client failed. fd=%d, fid=%s.", file_task.socket_handle, file_task.fid.c_str());
			protocol_family->destroy_protocol(protocol_file_info);
		}
	}
	else
		SLOG_WARN("can't find file task. fid=%s.", save_result.fid.c_str());
	//pthread_mutex_unlock(&m_filetask_lock);

	//删除任务
	file_task_delete(save_result.fid);
}

void ChunkWorker::on_file_req(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();

	ProtocolFileReq *protocol_file_req = (ProtocolFileReq*)protocol;
	FileReq &file_req = protocol_file_req->get_file_req();

	//1. 打开文件
	string local_file;
	DiskMgr::get_instance()->make_path(local_file, file_req.fid, file_req.index);
	int fd = open(local_file.c_str(), O_RDONLY);
	if(fd == -1)
	{
		SLOG_ERROR("open file error.file=%s.", local_file.c_str());
		return ;
	}
	struct stat file_stat;
	if(fstat(fd, &file_stat) == -1)
	{
		SLOG_ERROR("stat file error. errno=%d(%s)", errno, strerror(errno));
		close(fd);
		return ;
	}
	if(file_req.offset+file_req.size > file_stat.st_size)
	{
		SLOG_ERROR("file data error:offset=%d,size=%d,filesize=%d.",file_req.offset, file_req.size, file_stat.st_size);
		close(fd);
		return ;
	}
	lseek(fd, file_req.offset, SEEK_SET);

	//2 发送文件
	uint32_t READ_SIZE = 4096;
	uint32_t seg_offset = 0;
	uint32_t seg_size = 0;
	bool result = true;
	ByteBuffer byte_buffer(2048);
	ProtocolFile *protocol_file = NULL;
	while(seg_offset < file_req.size)
	{
		byte_buffer.clear();
		seg_size = file_req.size-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;

		//设置协议字段
		protocol_file = (ProtocolFile *)protocol_family->create_protocol(PROTOCOL_FILE);
		assert(protocol_file != NULL);
		FileSeg &file_seg = protocol_file->get_file_seg();
		file_seg.flag = FileSeg::FLAG_SEG;
		file_seg.fid = file_req.fid;
		file_seg.filesize = file_req.size;
		file_seg.size = seg_size;
		file_seg.offset = seg_offset;

		seg_offset += seg_size;

		if(!send_file_protocol_to_client(socket_handle, protocol_file, &byte_buffer, fd))
		{
			result = false;
			protocol_family->destroy_protocol(protocol_file);
			SLOG_ERROR("send file_seg failed.fid=%s.", file_req.fid.c_str());
			break;
		}
		protocol_family->destroy_protocol(protocol_file);
	}
	close(fd);

	if(result == false)
		return;

	//3 发送结束协议(该协议没有回复)
	protocol_file = (ProtocolFile *)protocol_family->create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	FileSeg &end_file_seg = protocol_file->get_file_seg();
	end_file_seg.flag = FileSeg::FLAG_END;
	end_file_seg.fid = file_req.fid;
	end_file_seg.filesize = file_req.size;
	end_file_seg.size = 0;
	if(!send_file_protocol_to_client(socket_handle, protocol_file, &byte_buffer, -1))
		SLOG_ERROR("send end_file_seg failed. fid=%s.", file_req.fid.c_str());
	protocol_family->destroy_protocol(protocol_file);
	return ;
}

///////////////////////////////  ChunkWorkerPool  //////////////////////////////////
Thread<SocketHandle>* ChunkWorkerPool::create_thread()
{
	ChunkWorker *chunk_worker = new ChunkWorker;
	return chunk_worker;
}

/////////////////////////////// Timer Handler  /////////////////////////////////
HANDLE_RESULT TimerHandler::on_timeout(int fd)
{
	SLOG_INFO("timer timeout...");
	m_demuxer->register_event(-1, EVENT_INVALID, 3000, this);
	return HANDLE_OK;
}
