/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#include "SFSFile.h"
#include "slog.h"
#include "TransProtocol.h"

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
	string fid="AAACCCDDD";

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
					if(!send_file_to_chunk(local_file, fid, chunk_path.ip, chunk_path.port))
					{
						file_info.result = FileInfo::RESULT_FAILED;
						break;
					}
				}
				return true;
			}
		case FileInfo::RESULT_SAVING:  //正在保存
			{
				SLOG_DEBUG("fid=%s is saving.", fid.c_str());
				//TODO sleep(1);  //等1s后重新请求master获取文件信息
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

bool File::send_file_protocol_to_chunk(TransSocket* trans_socket, ProtocolFile *protocol_file, ByteBuffer *byte_buffer, int fd)
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
	FileSeg& file_seg = protocol_file->get_file_seg();
	char *data_buffer = byte_buffer->get_append_buffer(file_seg.size);
	if(read(fd, data_buffer, file_seg.size) != file_seg.size)
	{
		SLOG_ERROR("read file error. errno=%d(%s).", errno, strerror(errno));
		return false;
	}
	byte_buffer->set_append_size(file_seg.size);
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

bool File::send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port)
{
	TransSocket trans_socket(chunk_addr.c_str(), chunk_port);
	if(!trans_socket.open(1000))
	{
		SLOG_ERROR("connect sfs failed.");
		return false;
	}

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

	int READ_SIZE = 4096;
	string filename = local_file.substr(local_file.find_last_of('/')+1);
	uint64_t filesize = file_stat.st_size;
	uint64_t seg_offset = 0;
	int seg_size = 0;
	bool result = true;

	ByteBuffer byte_buffer(2048);
	//TODO 发送开始协议

	ProtocolFile *protocol_file = (ProtocolFile *)m_protocol_family.create_protocol(PROTOCOL_FILE);
	assert(protocol_file != NULL);
	while(seg_offset < filesize)
	{
		byte_buffer.clear();
		seg_size = filesize-seg_offset;
		if(seg_size > READ_SIZE)
			seg_size = READ_SIZE;

		//设置协议字段
		FileSeg &file_seg = protocol_file->get_file_seg();
		file_seg.flag = FileSeg::FLAG_SEG;
		file_seg.fid = fid;
		file_seg.filesize = filesize;
		file_seg.size = seg_size;
		seg_offset += seg_size;

		if(!send_file_protocol_to_chunk(&trans_socket, protocol_file, &byte_buffer, fd))
		{
			result = false;
			break;
		}

		//接收数据
		ProtocolFileSaveResult *protocol_save_result = (ProtocolFileSaveResult*)m_protocol_family.create_protocol(PROTOCOL_FILE_SAVE_RESULT);
		assert(protocol_save_result != NULL);
		if(!TransProtocol::recv_protocol(&trans_socket, protocol_save_result))
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_save_result);
			break;
		}

		FileSaveResult &save_result = protocol_save_result->get_save_result();
		if(save_result.status != FileSaveResult::SEG_FAILED) //存储失败
		{
			result = false;
			m_protocol_family.destroy_protocol(protocol_save_result);
			break;
		}
		m_protocol_family.destroy_protocol(protocol_save_result);
	}

	if(result == true)
	{
		SLOG_INFO("send file succ. fid=%s.", fid.c_str());
		//TODO 发送结束协议
	}


	close(fd);
	return result;
}
