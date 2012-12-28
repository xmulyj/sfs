/*
 * SFSClient.h
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */

#ifndef _LIB_SFS_CLIENT_H_20121108
#define _LIB_SFS_CLIENT_H_20121108

#include "Socket.h"
#include "ByteBuffer.h"
#include "SFSProtocolFamily.h"

#include <stdint.h>
#include <string>
#include <vector>
using std::string;
using std::vector;

namespace SFS
{

class File
{
public:
	File(string &master_addr, int master_port, int n_replica);

	//获取fid的文件信息
	//-1: 请求失败
	//0: 请求成功, 有file info信息
	//1: 请求成功, 没有file info信息
	int get_file_info(string &fid, FileInfo &file_info);

	//从sfs读取fid文件保存到local_file中
	bool get_file(string &fid, string &local_file);

	//将文件local_file保存到sfs系统
	//失败返回false; 成功返回true,fileinfo表示保存后的文件信息
	bool save_file(FileInfo &fileinfo, string &local_file);
private:
	string m_master_addr;
	int    m_master_port;
	int    m_n_replica;
	SFSProtocolFamily m_protocol_family;
private:
	//查询文件信息, 返回值:false(请求失败), true(请求成功)
	bool _query_master(ProtocolFileInfoReq *protocol, ProtocolFileInfo::FileInfoResult &result, FileInfo &file_info);

	//获取fid的文件信息
	//query_chunk:当没有文件信息的时候是否请求分配chunk path
	//返回值:true(请求成功), false(请求失败)
	bool _get_file_info(string &fid, bool query_chunk, ProtocolFileInfo::FileInfoResult &fileinfo_result, FileInfo &file_info);

	bool send_file_to_chunk(string &local_file, string &fid, string &chunk_addr, int chunk_port);
	bool send_file_protocol_to_chunk(TransSocket* trans_socket, ProtocolFile *protocol_store, ByteBuffer *byte_buffer, int fd);
};

}
#endif //_LIB_SFS_CLIENT_H_20121108


