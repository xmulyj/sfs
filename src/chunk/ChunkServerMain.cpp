/*
 * MTServerAppFramework_main.cpp
 *
 *  Created on: 2012-9-11
 *      Author: xl
 */

#include "ChunkServer.h"
#include "ConfigReader.h"

ConfigReader* g_config_reader = NULL;
const char server_config[] = "config/server.config";

int main()
{
	SLOG_INIT("./config/slog.config");

	g_config_reader = new ConfigReader(server_config);
	ChunkServer chunk_server;
	chunk_server.start_server();

	SLOG_UNINIT();
	return 0;
}

