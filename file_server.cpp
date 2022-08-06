#include "file_server.h"
#include <stdio.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <string.h>
#include "utils.h"
#include "easylogging++.h"
#include <unistd.h>
#include <thread>
#include <sys/epoll.h> //epoll
#include <fcntl.h> //fcntl()
#include "worker.h"
#include "observer.h"
#include <cassert>

const int SEND_REGISTRY_INTERVAL = 10;


leveldb::DB* FileServer::m_db;
sockaddr_in FileServer::m_reg_addr;//registry地址
string FileServer::m_self_addr;
int FileServer::m_self_port;
string FileServer::m_machine_id;//机器Id
int FileServer::m_server_fd;//服务器socket描述符
Model  * FileServer::m_model;//模型
int FileServer::m_epoll_fd[MAX_WORKER_NUM];

atomic_uint64_t g_action_num(0),g_tcp_num(0);


void FileServer::SendToRegistryThread()
{
    while (true)
    {
       
        if(SendToRegistry()==Errors::Success)
        {
            sleep(SEND_REGISTRY_INTERVAL);
        }
        else
        {
            sleep(5);
        }

    }
}
Errors FileServer::SendToRegistry()
{
    

    Msg msg, result;

    msg.type = Types::AddFileServerRequest;
    msg.id = g_msg_id;
    AddServerReq * req = (AddServerReq*)msg.data;
    req->ip.ip = IpToInt(m_self_addr.data());
    req->ip.port = m_self_port;

    req->status.action_num = g_action_num;
    g_action_num = 0;
    req->status.tcp_num = g_tcp_num;
    req->status.cpu_rate = Observer::GetCpuUse();
    auto [total_memory,free_memory] = Observer::GetMemoryUse();
    req->status.total_memory = total_memory;
    req->status.free_memory = free_memory;


    auto [total_disk,free_disk] = Observer::GetDiskUse();
    req->status.total_disk = total_disk;
    req->status.free_disk = free_disk;

    if (RecvWithRetry(msg, (sockaddr*)&m_reg_addr, result, Types::AddFileServerResponse) == 0)
    {
        LOG(INFO) << "push to registry success" << endl;
        if(m_machine_id == "")
        {
            m_machine_id = result.data;
            LOG(INFO)<<"machine id:"<<m_machine_id<<endl;
        }
        return Errors::Success;
    }
    else
    {
        LOG(ERROR) << "recv from registry fail!" << errno << endl;
        return Errors::SendError;
    }

    
 

    
}
FileServer::FileServer(string _self_addr,int port,string reg_addr,int reg_port, string db_host, int db_port, string db_user, string db_pwd, string db_name)
{

   
	//开启推送服务
	m_reg_addr.sin_family = AF_INET;
	inet_pton(AF_INET, reg_addr.data(), &m_reg_addr.sin_addr);
	m_reg_addr.sin_port = htons(reg_port);


    m_self_addr = _self_addr;
    m_self_port = port;

    if(SendToRegistry()!=Errors::Success)
    {
        exit(0);
    }
    thread t(SendToRegistryThread);
    t.detach();

    //初始化模型
    m_model = new Model(db_host,db_user,db_pwd,db_name,db_port);
   

    //初始化socket
	int ret;
	m_server_fd = socket(AF_INET, SOCK_STREAM, 0); 
	if (m_server_fd < 0)
    {
		LOG(ERROR) << "Create socket fail! "<<errno << endl;
		return;
	}

    sockaddr_in self_addr;
    self_addr.sin_family = AF_INET;
    self_addr.sin_addr.s_addr = htonl(INADDR_ANY); 
    self_addr.sin_port = htons(port); 
	ret = bind(m_server_fd, (struct sockaddr*)&self_addr, sizeof(self_addr)); //bind
	if (ret == -1) 
    {
		LOG(ERROR) << "socket bind fail! "<<errno<<endl;
        exit(0);
	}

	ret = listen(m_server_fd, MAX_CLIENT_NUM); //listen
	if (ret == -1)
    {
		LOG(ERROR) << "Server: listen error!" << endl;
		exit(0);
	}
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options, "./oss", &m_db);
    if(!status.ok())
    {
        LOG(ERROR) << "init level db failed!" << endl;
		exit(0);
    }

    //初始化worker
    for(int i = 0;i<MAX_WORKER_NUM;i++)
    {
        thread t(WorkerThread,i);
        t.detach();
    }

   
}

void FileServer::WorkerThread(int epoll_index)
{
    m_epoll_fd[epoll_index] = epoll_create(MAX_CLIENT_NUM+1);
	if (m_epoll_fd[epoll_index] == -1)
     {
		LOG(ERROR) << "Server: create epoll error!" << endl;
		exit(0);
	}
    Worker worker(m_reg_addr,m_epoll_fd[epoll_index],m_model,m_machine_id,m_db);
    worker.Run();
}

void FileServer::AddFd(int epoll_fd, int sock_fd) 
{
    g_tcp_num++;
	epoll_event event;
	event.data.fd = sock_fd;
	event.events = EPOLLIN;
    event.events |= EPOLLET;
    event.events |= EPOLLONESHOT;
	epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock_fd, &event);
	
    int old_option = fcntl(sock_fd, F_GETFL); //设置为非阻塞模式
	int new_option = old_option | O_NONBLOCK;
	fcntl(sock_fd, F_SETFL, new_option);
}

void FileServer::Run()
{
    
    int current_worker = 0;
    sockaddr_in client_addr;
    socklen_t len = sizeof(sockaddr_in);
    while (true)
    {
        int conn_fd = accept(m_server_fd, (struct sockaddr*)&client_addr, &len);
        if (conn_fd < 0)
        {
            LOG(ERROR)<< "accept error! errno"<<errno << endl;
            continue;
        }
        LOG(INFO)<<IpToDot(htonl(client_addr.sin_addr.s_addr))<<":"<<htons(client_addr.sin_port)<<" connected,fd:"<<conn_fd<<endl;
        AddFd(m_epoll_fd[current_worker++], conn_fd); //注册到对应的worker
        if(current_worker == MAX_WORKER_NUM)
        {
            current_worker = 0;
        }
    }

}

FileServer::~FileServer()
{
    close(m_server_fd);
    delete m_model;
}

