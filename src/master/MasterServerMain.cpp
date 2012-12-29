/*
 * DownloadServer_main.cpp
 *
 *  Created on: 2012-9-16
 *      Author: LiuYongjin
 */

#include "MasterServer.h"
#include "ListenHandler.h"
#include "Socket.h"
#include "IODemuxerEpoll.h"

#include "ConfigReader.h"
ConfigReader *g_config_reader = NULL;
const char config_path[] = "config/server.config";

int main()
{
	SLOG_INIT("./config/slog.config");

	g_config_reader = new ConfigReader(config_path);

	int master_port = g_config_reader->GetValueInt("MasterPort", 3012);
	ListenSocket linsten_socket(master_port);
	if(!linsten_socket.open())
	{
		SLOG_ERROR("listen on port:%d error.", master_port);
		return -1;
	}

	MasterThreadPool server_pool(1);
	server_pool.start();

	//listen event
	ListenHandler listen_handler(&server_pool);
	EpollDemuxer io_demuxer;
	io_demuxer.register_event(linsten_socket.get_handle(), EVENT_READ|EVENT_PERSIST, -1, &listen_handler);

	//run server forever
	io_demuxer.run_loop();

	SLOG_UNINIT();
	return 0;
}


