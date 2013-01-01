/*
 * SFSClient.cpp
 *
 *  Created on: 2012-11-8
 *      Author: LiuYongJin
 */
#include <string>
using std::string;
#include "slog.h"
#include "SFSFile.h"

int main(int agrc, char* argv[])
{
	SLOG_INIT(NULL);
	//get gapth
	string master_addr="127.0.0.1";
	int master_port = 3012;
	SFS::File sfs_file(master_addr, master_port, 2);

	/*
	//file info
	string fid="AAACCCDDD";
	SFS::FileInfo fileinfo;
	if(sfs_file.get_file_info(&fileinfo, fid))
	{
		SLOG_INFO("result:%d FID:%s FileSize:%lld.", fileinfo.result, fileinfo.fid.c_str(), fileinfo.size);
		vector<ChunkInfo>::iterator it;
		for(it=fileinfo.chunkinfo.begin(); it!=fileinfo.chunkinfo.end(); ++it)
			SLOG_INFO("ChunkInfo:ChunkPath:%s ChunkAdd:%s ChunkPort:%d.",it->path.c_str(), it->chunk_addr.c_str(), it->port);
	}
	*/

	string filename="/data/test.txt";
	FileInfo file_info;
	file_info.result = FileInfo::RESULT_INVALID;

	if(sfs_file.save_file(file_info, filename) && file_info.result==FileInfo::RESULT_SUCC)
	{
		SLOG_INFO("save file succ. filename=%s. fid=%s, size=%d.", file_info.fid.c_str(), file_info.size);
		int i;
		for(i=0; i<file_info.get_chunkpath_count(); ++i)
		{
			ChunkPath &chunk_path = file_info.get_chunkpath(i);
			SLOG_INFO("chunk_path[%d]: id=%s, ip=%s, port=%d, %d_%lld."
						, i
						, chunk_path.id.c_str()
						, chunk_path.ip.c_str()
						, chunk_path.port
						, chunk_path.index
						, chunk_path.offset);
		}
	}
	else
		SLOG_ERROR("save file failed.filename=%s, result=%d", filename.c_str(), file_info.result);

	SLOG_UNINIT();
	return 0;
}


