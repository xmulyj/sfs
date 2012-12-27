/*
 * MTServerAppFramework.cpp
 *
 *  Created on: 2012-9-11
 *      Author: xl
 */

#include "ChunkWorker.h"
#include "IODemuxerEpoll.h"
#include "slog.h"

#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>

///////////////////////////////  ChunkWorker  //////////////////////////////////
bool ChunkWorker::start_server()
{
	//Init NetInterface
	init_net_interface();
	set_thread_ready();

	////Add your codes here
	///////////////////////
	m_disk_path = "/data/sfs_chunk";  //数据存放路径
	//pthread_mutex_init(&m_filetask_lock, NULL);
	load_disk_files();
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
bool ChunkWorker::file_task_find(string &fid)
{
	bool find;
	pthread_mutex_lock(&m_filetask_lock);
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
		file_task.buf = malloc(file_seg.filesize);
		if(file_task.buf == NULL)
		{
			SLOG_ERROR("create file task failed:no memory.fid=%s, file_name=%s, file_size=%lld, seg_size=%d."
						, file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize, file_seg.size);
		}
		else
		{
			result = true;
			m_filetask_map.insert(std::make_pair(file_task.fid, file_task));  //保存任务
			SLOG_DEBUG("create file task succ.fid=%s, file_name=%s, file_size=%lld, seg_size=%d."
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
		SLOG_DEBUG("delete file task succ:fid=%s, name=%s, size=%lld", file_task.fid.c_str(), file_task.name.c_str(), file_task.size);
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
		SLOG_DEBUG("file_task:fid=%s, size=%ldd. file_seg:total_size=%ldd, offset=%ldd, size=%d."
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
	FileTaskMap::iterator it = m_filetask_map.find(fid);
	if(it != m_filetask_map.end())
	{
		FileTask &file_task = it->second;
		//保存到文件中
		//生成file_info
		//向master上报file_info
	}
	else
		SLOG_WARN("save file task failed:can't find task[fid=%s].", fid.c_str());

	//pthread_mutex_unlock(&m_filetask_lock);
}

//////////////////////////  disk file  /////////////////////////
void ChunkWorker::load_disk_files()
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
			SLOG_ERROR("not dir.sub_dir=%s, error:%s", sub_dir.c_str(), strerror(errno));
			continue;
		}
		else
		{
			struct dirent* ent = NULL;
			DIR *dir;
			if((dir=opendir(sub_dir.c_str())) == NULL)
			{
				SLOG_ERROR("open dir error. sub_dir=%s, error:%s", sub_dir.c_str(), strerror(errno));
				continue;
			}
			while((ent=readdir(dir))!=NULL)  //计算文件数
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
			if((m_disk_files[i].fp = fopen(name, "w")) == NULL)
			{
				SLOG_ERROR("open file error. file=%s, error:%s", name, strerror(errno));
				continue;
			}
			fseek(m_disk_files[i].fp, 0, SEEK_END);
			m_disk_files[i].cur_pos = ftell(m_disk_files[i].fp);
			SLOG_DEBUG("load file succ. name=%s, size=%ldd.", name, m_disk_files[i].cur_pos);
		}
	}
}

///////////////////////////////////////////////////////////
//响应客户端发送文件数据包
void ChunkWorker::on_file(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();
	ProtocolFileStatus* protocol_file_status = NULL;

	ProtocolFile *protocol_file = (ProtocolFile *)protocol;
	ProtocolFile::FileFlag file_flag = protocol_file->get_flag();
	FileSeg &file_seg = protocol_file->get_file_seg();
	SLOG_INFO("receive File Protocol[file info: flag=%d, fid=%s, name=%s, filesize=%lld] [seg info: offset=%lld, index=%d, size=%d]."
				,file_flag, file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize, file_seg.offset, file_seg.index, file_seg.size);

	switch(file_flag)
	{
		case ProtocolFile::FLAG_START:
		{
			protocol_file_status = (ProtocolFileStatus*)protocol_family->create_protocol(PROTOCOL_FILE_STATUS);
			assert(protocol_file_status != NULL);
			FileSeg &file_seg_resp = protocol_file_status->get_file_seg();
			file_seg_resp.fid = file_seg.fid;
			if(file_task_find(file_seg.fid))  //已经存在
			{
				SLOG_WARN("file task already started. fid=%s.", file_seg.fid.c_str());
				protocol_file_status->set_status(ProtocolFileStatus::CREATE_FAILED);
			}
			else if(!file_task_create(socket_handle, file_seg))  //创建任务失败
			{
				SLOG_ERROR("create file task failed. fid=%s.", file_seg.fid.c_str());
				protocol_file_status->set_status(ProtocolFileStatus::CREATE_FAILED);
			}
			else  //成功
			{
				SLOG_INFO("create file task succ. fid=%s, name=%s, size=%ldd." ,file_seg.fid.c_str(), file_seg.name.c_str(), file_seg.filesize);
				protocol_file_status->set_status(ProtocolFileStatus::CREATE_SUCC);
			}
			break;
		}
		case ProtocolFile::FLAG_SEG:  //文件分片
		{
			protocol_file_status = (ProtocolFileStatus*)protocol_family->create_protocol(PROTOCOL_FILE_STATUS);
			assert(protocol_file_status != NULL);
			FileSeg &file_seg_resp = protocol_file_status->get_file_seg();
			file_seg_resp.fid = file_seg.fid;
			protocol_file_status->set_status(ProtocolFileStatus::SEG_SUCC);
			if(!file_task_save(file_seg))  //失败
			{
				SLOG_ERROR("save file seg failed. fid=%s.", file_seg.fid.c_str());
				protocol_file_status->set_status(ProtocolFileStatus::SEG_FAILED);
				file_task_delete(file_seg.fid);
			}
			break;
		}
		case ProtocolFile::FLAG_END:  //已经结束
		{
			SLOG_INFO("file task finished. fid=%s.", file_seg.fid.c_str());
			//保存文件
			if(!save_file(file_seg.fid))
				SLOG_ERROR("save file failed. fid=%s.", file_seg.fid.c_str());
			return;
		}
	}//switch

	if(!send_protocol(socket_handle, protocol_file_status))
	{
		SLOG_ERROR("send file status protocol failed. fd=%d, fid=%s.", socket_handle, file_seg.fid.c_str());
		protocol_family->destroy_protocol(protocol_file_status);
		file_task_delete(file_seg.fid);
	}
}

//响应master回复file_info保存结果
void ChunkWorker::on_file_info_save_result(SocketHandle socket_handle, Protocol *protocol)
{
	SFSProtocolFamily* protocol_family = (SFSProtocolFamily*)get_protocol_family();

	ProtocolFileInfoSaveResult *protocol_save_result = (ProtocolFileInfoSaveResult*)protocol;
	ProtocolFileInfoSaveResult::FileInfoSaveResult save_result = protocol_save_result->get_result();
	string fid = protocol_save_result->get_fid();
	SLOG_INFO("fid=%s, save_result=%d.", fid.c_str(), save_result);

	//pthread_mutex_lock(&m_filetask_lock);
	FileTaskMap::iterator it = m_filetask_map.find(fid);
	if(it != m_filetask_map.end())
	{
		FileTask &file_task = it->second;
		ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo*)protocol_family->create_protocol(PROTOCOL_FILE_INFO);
		protocol_file_info->set_result(ProtocolFileInfo::FILE_INFO_FAILED);
		if(save_result == ProtocolFileInfoSaveResult::SAVE_RESULT_SUCC)  //master保存成功
		{
			protocol_file_info->set_result(ProtocolFileInfo::FILE_INFO_SUCC);
			FileInfo &file_info = protocol_file_info->get_fileinfo();
			file_info = file_task.file_info;
			ChunkPath &chunk_path = file_info.get_path(0);
			SLOG_DEBUG("chunk[%d]:id=%s, ip=%s, port=%d, index=%d, offset=%lld."
						,0, chunk_path.id.c_str(), chunk_path.addr.c_str(), chunk_path.port, chunk_path.index, chunk_path.offset);
		}

		if(!send_protocol(file_task.socket_handle, protocol_file_info))
		{
			SLOG_ERROR("send file info to client failed. fd=%d, fid=%s.", file_task.socket_handle, file_task.fid.c_str());
			protocol_family->destroy_protocol(protocol_file_info);
		}
	}
	else
		SLOG_WARN("can't find file task. fid=%s.", fid.c_str());
	//pthread_mutex_unlock(&m_filetask_lock);
	//删除任务
	file_task_delete(fid);
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
