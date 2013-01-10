/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#include "SFSFile.h"
#include "slog.h"
#include "TransProtocol.h"
#include "sha1.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>

using namespace SFS;

File::File(string &master_addr, int master_port, int n_replica)
	:m_master_addr(master_addr)
	,m_master_port(master_port)
	,m_n_replica(n_replica)
{}

bool File::get_file_info(string &fid, FileInfo &file_info)
{
	return _get_file_info(fid, false, file_info);
}

bool File::save_file(FileInfo &file_info, string &local_file)
{
	string fid;
	int retry_time = 0;

	if(!SHA1::hash(local_file, fid))
	{
		SLOG_ERROR("get fid failed. file=%s.", local_file.c_str());
		return false;
	}
	SLOG_DEBUG("get fid: file=%s fid=%s.", local_file.c_str(), fid.c_str());

	while(true)
	{
		if(!_get_file_info(fid, true, file_info))
		{
			SLOG_DEBUG("query master error.");
			return false;
		}

		switch(file_info.result)
		{
		case FileInfo::RESULT_FAILED:  //失败
			{
				SLOG_DEBUG("get file info failed. fid=%s.", fid.c_str());
				return true;
			}
		case FileInfo::RESULT_SUCC:  //已经保存过
			{
				SLOG_DEBUG("file already exist. fid:%s, name:%s, size:%d.", file_info.fid.c_str(), file_info.name.c_str(), file_info.size);
				return true;
			}
		case FileInfo::RESULT_CHUNK:  //分配chunk成功
			{
				int i;
				for(i=0; i<file_info.get_chunkpath_count(); ++i)
				{
					ChunkPath &chunk_path = file_info.get_chunkpath(i);
					SLOG_INFO("request chunk to store file. fid=%s. chunk_ip:%s, chunk_port:%d", fid.c_str(), chunk_path.ip.c_str(), chunk_path.port);
					FileInfo temp_file_info;
					if(!_send_file_to_chunk(local_file, fid, chunk_path.ip, chunk_path.port, temp_file_info))
					{
						file_info.result = FileInfo::RESULT_FAILED;
						break;
					}
					else
						file_info = temp_file_info;
				}
				return true;
			}
		case FileInfo::RESULT_SAVING:  //正在保存
			{
				++retry_time;
				if(retry_time > 3)
				{
					SLOG_DEBUG("fid=%s is saving. reach the max retry_times.", fid.c_str(), retry_time);
					return true;
				}
				SLOG_DEBUG("fid=%s is saving, waiting for file_info. retry_time=%d.", fid.c_str(), retry_time);
				sleep(2);  //等1s后重新请求master获取文件信息
				break;
			}
		default:
			{
				SLOG_WARN("unknown result value:result=%d.", file_info.result);
				return false;
			}
		}
	}

	return true;
}

bool File::get_file(string &fid, string &local_file)
{
	FileInfo file_info;
	file_info.result = FileInfo::RESULT_INVALID;
	if(!_get_file_info(fid, false, file_info) || file_info.result!=FileInfo::RESULT_SUCC)
	{
		SLOG_DEBUG("get file info failed. fid=%s, result=%d.", fid.c_str(), file_info.result);
		return false;
	}

	int chunk_count = file_info.get_chunkpath_count();
	assert(chunk_count > 0);
	int i;
	bool finish = false;
	for(i=0; i<chunk_count&&!finish; ++i)
	{
		ChunkPath &chunk_path = file_info.get_chunkpath(i);
		TransSocket trans_socket(chunk_path.ip.c_str(), chunk_path.port);
		if(!trans_socket.open(1000))
		{
			SLOG_ERROR("can't connect chunk:ip=%s, port=%d.", chunk_path.ip.c_str(), chunk_path.port);
			continue;
		}
		ProtocolFileReq *protocol_filereq = (ProtocolFileReq*)m_protocol_family.create_protocol(PROTOCOL_FILE_REQ);
		assert(protocol_filereq != NULL);
		FileReq &file_req = protocol_filereq->get_file_req();
		file_req.fid = file_info.fid;
		file_req.size = file_info.size;
		file_req.index = chunk_path.index;
		file_req.offset = chunk_path.offset;
		bool send_ok = TransProtocol::send_protocol(&trans_socket, protocol_filereq);
		m_protocol_family.destroy_protocol(protocol_filereq);
		if(!send_ok)
		{
			SLOG_ERROR("sent file_req protocol failed.");
			continue;
		}

		int fd = open(local_file.c_str(), O_WRONLY|O_CREAT);
		if(fd == -1)
		{
			SLOG_ERROR("open file failed. file=%s. errno=%d(%s). ", local_file.c_str(), errno, strerror(errno));
			continue;
		}
		//接收ProtocolFile
		while(1)
		{
			//TODO
			ProtocolFile *protocol_file = (ProtocolFile*)m_protocol_family.create_protocol(PROTOCOL_FILE);
			assert(protocol_file != NULL);
			if(!TransProtocol::recv_protocol(&trans_socket, protocol_file))
			{
				m_protocol_family.destroy_protocol(protocol_file);
				SLOG_ERROR("receive file_seg failed. fid=%s.", file_info.fid.c_str());
				break;
			}
			FileSeg &file_seg = protocol_file->get_file_seg();
			if(file_seg.flag == FileSeg::FLAG_END)  //完成
			{
				SLOG_INFO("get data finished. fid=%s.", file_info.fid.c_str());
				finish = true;
				break;
			}
			else if(file_seg.flag != FileSeg::FLAG_SEG)
			{
				SLOG_ERROR("get data from chunk error. fid=%s.", file_info.fid.c_str());
				m_protocol_family.destroy_protocol(protocol_file);
				break;
			}

			if(!_save_fileseg_to_file(fd, file_seg))
			{
				SLOG_ERROR("save data to file error.");
				m_protocol_family.destroy_protocol(protocol_file);
				break;
			}
			m_protocol_family.destroy_protocol(protocol_file);
		}
		close(fd);
	}

	return true;
}

///////////////////////////////////////////////////////
bool File::_query_master(ProtocolFileInfoReq *protocol, FileInfo &file_info)
{
	TransSocket trans_socket(m_master_addr.c_str(), m_master_port);
	if(!trans_socket.open(1000))
	{
		SLOG_ERROR("connect master failed.");
		return false;
	}
	//发送协议
	if(!TransProtocol::send_protocol(&trans_socket, protocol))
		return false;
	//接收协议
	ProtocolFileInfo *protocol_fileinfo = (ProtocolFileInfo *)m_protocol_family.create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_fileinfo != NULL);
	bool temp = TransProtocol::recv_protocol(&trans_socket, protocol_fileinfo);
	if(temp)  //请求成功
		file_info = protocol_fileinfo->get_fileinfo();
	m_protocol_family.destroy_protocol(protocol_fileinfo);

	return temp;
}

bool File::_get_file_info(string &fid, bool query_chunk, FileInfo &fileinfo)
{
	//协议数据
	ProtocolFileInfoReq* protocol_fileinfo_req = (ProtocolFileInfoReq*)m_protocol_family.create_protocol(PROTOCOL_FILE_INFO_REQ);
	assert(protocol_fileinfo_req != NULL);

	protocol_fileinfo_req->get_fid() = fid;
	protocol_fileinfo_req->get_query_chunkpath() = query_chunk;

	bool result = _query_master(protocol_fileinfo_req, fileinfo);
	m_protocol_family.destroy_protocol(protocol_fileinfo_req);

	return result;
}

bool File::_send_file_protocol_to_chunk(TransSocket* trans_socket, ProtocolFile *protocol_file, ByteBuffer *byte_buffer, int fd)
{
	byte_buffer->clear();

	//1 预留头部空间
	int header_length;
	ProtocolHeader *header = protocol_file->get_protocol_header();
	header_length = header->get_header_length();
	byte_buffer->reserve(header_length);
	//2 编码协议体
	if(!protocol_file->encode_body(byte_buffer))
	{
		SLOG_ERROR("encode body error");
		return false;
	}
	//3 添加数据
	if(fd > 0)
	{
		FileSeg& file_seg = protocol_file->get_file_seg();
		char *data_buffer = byte_buffer->get_append_buffer(file_seg.size);
		if(read(fd, data_buffer, file_seg.size) != file_seg.size)
		{
			SLOG_ERROR("read file error. errno=%d(%s).", errno, strerror(errno));
			return false;
		}
		byte_buffer->set_append_size(file_seg.size);
	}
	//4. 编码协议头
	int body_length = byte_buffer->size()-header_length;
	char *header_buffer = byte_buffer->get_data(0, header_length);
	if(!header->encode(header_buffer, body_length))
	{
		SLOG_ERROR("encode header error");
		return false;
	}
	//5. 发送数据
	if(trans_socket->send_data_all(byte_buffer->get_data(), byte_buffer->size()) == TRANS_ERROR)
	{
		SLOG_ERROR("send data error");
		return false;
	}

	return true;
}

bool File::_send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port, FileInfo &file_info)
{
	TransSocket trans_socket(chunk_addr.c_str(), chunk_port);
	if(!trans_socket.open(1000))
	{
		SLOG_ERROR("connect sfs failed.");
		return false;
	}
	//1. 获取文件大小
	int fd = open(local_file.c_str(), O_RDONLY);
	if(fd == -1)
	{
		SLOG_ERROR("open file error.");
		return false;
	}
	struct stat file_stat;
	if(fstat(fd, &file_stat) == -1)
	{
		SLOG_ERROR("stat file error. errno=%d(%s)", errno, strerror(errno));
		close(fd);
		return false;
	}

	//发送文件
	int READ_SIZE = 4096;
	string filename = local_file.substr(local_file.find_last_of('/')+1);
	uint32_t filesize = file_stat.st_size;
	uint32_t seg_offset = 0;
	int seg_size = 0;
	bool result = true;

	ByteBuffer byte_buffer(2048);
	ProtocolFile *protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)m_protocol_family.create_protocol(PROTOCOL_FILE_SAVE_RESULT);
	assert(protocol_save_result != NULL);

	//1 发送开始协议
	FileSeg &start_file_seg = protocol_file->get_file_seg();
	start_file_seg.flag = FileSeg::FLAG_START;
	start_file_seg.fid = fid;
	start_file_seg.name = filename;
	start_file_seg.filesize = filesize;
	start_file_seg.size = 0;
	if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, -1))
	{
		SLOG_ERROR("send start_file_seg failed. fid=%s.", fid.c_str());
		m_protocol_family.destroy_protocol(protocol_file);
		return false;
	}
	if(!TransProtocol::recv_protocol(&trans_socket, protocol_save_result))
	{
		SLOG_ERROR("receive start_file_seg resp failed. fid=%s.", fid.c_str());
		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
		return false;
	}
	FileSaveResult &save_result = protocol_save_result->get_save_result();
	if(save_result.status == FileSaveResult::CREATE_FAILED) //存储失败
	{
		SLOG_ERROR("chunk create file failed. fid=%s.", fid.c_str());
		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
		return false;
	}
	m_protocol_family.destroy_protocol(protocol_file);
	m_protocol_family.destroy_protocol(protocol_save_result);

	//2 发送分片
	while(seg_offset < filesize)
	{
		byte_buffer.clear();
		seg_size = filesize-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;

		//设置协议字段
		protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
		assert(protocol_file != NULL);
		FileSeg &file_seg = protocol_file->get_file_seg();
		file_seg.set(FileSeg::FLAG_SEG, fid, filename, filesize, seg_offset, 0, seg_size);
		seg_offset += seg_size;

		if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, fd))
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_file);

			SLOG_ERROR("send file_seg failed.fid=%s.", fid.c_str());
			break;
		}

		//接收存储结果
		ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)m_protocol_family.create_protocol(PROTOCOL_FILE_SAVE_RESULT);
		assert(protocol_save_result != NULL);
		if(!TransProtocol::recv_protocol(&trans_socket, protocol_save_result))
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_file);
			m_protocol_family.destroy_protocol(protocol_save_result);

			SLOG_ERROR("receive file_seg_save_result failed. fid=%s.", fid.c_str());
			break;
		}

		FileSaveResult &save_result = protocol_save_result->get_save_result();
		if(save_result.status == FileSaveResult::SEG_FAILED) //存储失败
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_save_result);

			SLOG_ERROR("chunk save file_seg failed. fid=%s.", fid.c_str());
			break;
		}

		m_protocol_family.destroy_protocol(protocol_file);
		m_protocol_family.destroy_protocol(protocol_save_result);
	}
	close(fd);

	if(result == false)
		return false;

	//3 发送结束协议(该协议没有回复)
	protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	FileSeg &end_file_seg = protocol_file->get_file_seg();
	end_file_seg.flag = FileSeg::FLAG_END;
	end_file_seg.fid = fid;
	end_file_seg.filesize = filesize;
	end_file_seg.size = 0;
	if(!_send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, -1))
	{
		SLOG_ERROR("send end_file_seg failed. fid=%s.", fid.c_str());
		m_protocol_family.destroy_protocol(protocol_file);
		return false;
	}
	m_protocol_family.destroy_protocol(protocol_file);

	//等待chunk回复file_info信息
	ProtocolFileInfo *protocol_file_info = (ProtocolFileInfo*)m_protocol_family.create_protocol(PROTOCOL_FILE_INFO);
	assert(protocol_file_info != NULL);
	if(!TransProtocol::recv_protocol(&trans_socket, protocol_file_info))
	{
		SLOG_ERROR("receive file_info failed. fid=%s.", fid.c_str());
		m_protocol_family.destroy_protocol(protocol_file_info);
		return false;
	}
	file_info = protocol_file_info->get_fileinfo();
	m_protocol_family.destroy_protocol(protocol_file_info);

	return true;
}

bool File::_save_fileseg_to_file(int fd, FileSeg &file_seg)
{
	lseek(fd, file_seg.offset, SEEK_SET);
	ssize_t size = write(fd, file_seg.data, file_seg.size);
	return size == file_seg.size;
}
