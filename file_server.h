#pragma once
#include <iostream>
#include <list>
#include "utils.h"
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <set>
#include "database.h"
#include "model.h"
#include <leveldb/db.h>


using namespace std;
const int MAX_WORKER_NUM = 8;
const int MAX_CLIENT_NUM = 100000;

class FileServer {

private:
	static leveldb::DB* m_db;
	static sockaddr_in m_reg_addr;//registry地址
	static string m_self_addr;
	static int m_self_port;
	
	static string m_machine_id;//机器Id
	static int m_server_fd;//服务器socket描述符
	static Model  * m_model;//模型
	static int m_epoll_fd[MAX_WORKER_NUM];


private:
	static void WorkerThread(int epoll_index);
	static Errors SendToRegistry();
	static void SendToRegistryThread();
	static void AddFd(int epoll_fd, int sock_fd);

public:
	FileServer(string self_addr,int port,string reg_addr,int reg_port,string db_host,int db_port,string db_user,string db_pwd,string db_name);
	void Run();
	~FileServer();
	
};

