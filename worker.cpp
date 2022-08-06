#include "worker.h"
#include "easylogging++.h"
#include <sys/epoll.h> //epoll
#include <fcntl.h> //fcntl()
#include <thread>
#include <sys/sendfile.h>

MsgDesc::MsgDesc(int _fd)
{
	fd = _fd;
	done = true;
	offset = total_size = 0;
}

Worker::Worker(sockaddr_in registry_addr,int epoll_fd,Model * model,string machine_id,leveldb::DB* db)
{
	m_db = db;
	m_epoll_fd = epoll_fd;
	m_model = model;
	m_machine_id = machine_id;
	m_registry_addr = registry_addr;
	sem_init(&m_ready_fd_sem,0,0);
	sem_init(&m_backup_sem,0,0);
	for(int i=1;i<=MAX_WORK_THREAD;i++)//启动处理线程
	{
		thread work_thread(&Worker::WorkThread,this);
		work_thread.detach();
	}
	for(int i=1;i<=BACKUP_THREAD;i++)
	{
		thread backup_thread(&Worker::BackupThread,this);
		backup_thread.detach();
	}


}
bool Worker::RecvOneMsg(MsgDesc *msg_desc,Msg & msg)
{

	int read_size = 0;
	while(read_size<sizeof(Msg)) //读取一个msg
	{
		int ret  = recv(msg_desc->fd,(char*)&msg+read_size,sizeof(Msg)-read_size,0);
		if(ret < 0)
		{
			if(errno == EAGAIN || errno == EWOULDBLOCK)//数据全部读取完毕
			{
				
				if(read_size == 0)//如果是一开始读
				{
					LOG(INFO)<<"fd:"<<msg_desc->fd<<" read end,reset"<<endl;
					ResetFd(msg_desc->fd);
					return true;
				}
				else
				{
					continue;//如果msg读到一半缓冲区没数据了，继续读
				}
				
			}
			else//真的发生错误了
			{
				LOG(ERROR)<<"socket recv error:"<<errno<<endl;
				RemoveFd(msg_desc->fd);
				return true;
			}
		}
		else if(ret == 0)//对方断开连接
		{
			
			RemoveFd(msg_desc->fd);
			return true;
		}
		else
		{
			read_size+=ret;
		}
	}

	msg_desc->type = msg.type;
	switch (msg.type)
	{
	case Types::AddFileRequest:
	{
		AddFileReq * req = (AddFileReq*)msg.data;
		msg_desc->file_id = req->file_id;
		msg_desc->total_size = req->file_size;
		msg_desc->offset = 0;
		msg_desc->done = false;
		break;
	}
	case Types::GetFileRequest:
	{
		GetFileReq * req = (GetFileReq*)msg.data;
		msg_desc->file_id = req->file_id;
		msg_desc->offset = req->offset;
		msg_desc->done = false;
		break;
	}
	case Types::AddBackupFileRequest:
	{
		AddBackupFileReq * req = (AddBackupFileReq *)msg.data;
		msg_desc->file_id = req->file_id;
		msg_desc->ak = req->ak;
		msg_desc->total_size = req->file_size;
		msg_desc->offset = 0;
		msg_desc->done = false;
	}
	default:
		break;
	}
	msg_desc->begin = true;
	
	return false;
}
void Worker::WorkThread()
{
	Msg msg;
	int read_size;
	int ret;
	MsgDesc * msg_desc;
	bool read_over;
	uint64_t handle_size;
	while(true)
	{
		sem_wait(&m_ready_fd_sem);
		if(m_ready_fd_q.try_dequeue(msg_desc))
		{
			handle_size = 0;
			read_over = false;
			while(handle_size < MAX_HANDLE_SIZE && !read_over)//处理任务的循环
			{
				if(msg_desc->done)//上个任务处理完了
				{
					
					read_over = RecvOneMsg(msg_desc,msg);
				}
			
				if(!read_over)
				{
					g_action_num++;
					switch (msg_desc->type)
					{
					case Types::LoginRequest:
						ret = Login(msg_desc->fd,msg);
						if(ret<0)
						{
							read_over = true;
							RemoveFd(msg_desc->fd);
						}
						break;
					case Types::AddFileRequest:
						if(CheckLogin(msg_desc->fd))
						{
							ret = AddFile(msg_desc,1);
							if(ret<0)
							{
								read_over = true;
								RemoveFd(msg_desc->fd);
							}
							handle_size += ret;
						}
						else
						{
							read_over = true;
						}
						break;
					case Types::GetFileRequest:
						if(CheckLogin(msg_desc->fd))
						{
							ret = GetFile(msg_desc);
							if(ret<0)
							{
								read_over = true;
								RemoveFd(msg_desc->fd);
							}
							handle_size += ret;
						}
						else
						{
							read_over = true;
						}
						break;
					case Types::AddBackupFileRequest:
						ret = AddFile(msg_desc,0);
						if(ret<0)
						{
							read_over = true;
							RemoveFd(msg_desc->fd);
						}
						handle_size += ret;
						break;
					default:
						LOG(ERROR)<<"unknow msg type:"<<(int)msg.type<<endl;
						msg_desc->done = true;
						break;
					}
				}
			
			}
			
				
				
			

			if(!read_over) //达到最大处理数据量了，但是仍然没有处理完
			{
				m_ready_fd_q.enqueue(msg_desc);
				sem_post(&m_ready_fd_sem);
			}
			else
			{
				delete msg_desc;
			}
		}
		else
		{
			LOG(WARNING)<<"dequeue failed,retry sem_post"<<endl;
			sem_post(&m_ready_fd_sem);
		}
		
	}
}
void Worker::Run()
{
	epoll_event events[MAX_EVENT_NUMBER];
	while (1) 
	{
		int ret = epoll_wait(m_epoll_fd, events, MAX_EVENT_NUMBER, -1); //epoll_wait
		if (ret < 0) 
		{
			LOG(ERROR) << "Server: epoll error" << endl;
			break;
		}
		for (int i = 0; i < ret; ++i)
		{
			int sockfd = events[i].data.fd;
			if (events[i].events & EPOLLIN)
			{ 
				MsgDesc * msg_desc = new MsgDesc(sockfd);
				m_ready_fd_q.enqueue(msg_desc);
				sem_post(&m_ready_fd_sem);
			}   
			else
			{
				LOG(WARNING)<<"epoll event not epollin"<<endl;
			}//是in事件
		}//一次wait多个事件的循环
	}//主while循环
}
bool Worker::CheckLogin(int fd)
{
	m_aks_mtx.lock_shared();
	if(m_aks[fd]=="")
	{
		m_aks_mtx.unlock_shared();
		LOG(WARNING)<<"fd:"<<fd<<" not login but try to do something"<<endl;
		RemoveFd(fd);
		return false;
	}
	else
	{
		m_aks_mtx.unlock_shared();
		return true;
	}
}

int Worker::Login(int fd,Msg &msg)//登陆
{

	LOG(INFO)<<"fd:"<<fd<<":login"<<endl;
	LoginReq* req = (LoginReq*)msg.data;
	string ak = req->ak,sk = req->sk;
	Msg response;
	response.type = Types::LoginResponse;
	response.id = msg.id;
	LoginRes * res = (LoginRes*)response.data;
	res->err = m_model->Login(ak,sk);
	if(res->err == Errors::Success)
	{
		m_aks_mtx.lock();
		m_aks[fd] = ak;
		m_aks_mtx.unlock();
	}
	if(res->err == Errors::AuthFail)
	{
		strcpy(res->msg,"ak or/and sk error");
	}
	if(send(fd,&response,sizeof(Msg),0)<0)
	{
		LOG(ERROR)<<"send to client error:"<<errno<<endl;
		return -1;
	}
	return 0;
	
}

int64_t Worker::AddFile(MsgDesc * msg,bool need_back_up)
{

	if(msg->total_size>MAX_CACHE_SIZE)
	{
		if(msg->begin)//如果是刚开始
		{
			LOG(INFO) << "fd:"<< msg->fd << " begin to send file:" << msg->file_id << endl;
			msg->begin = false;
			m_aks_mtx.lock_shared();
			string file_path = "./"+m_aks[msg->fd];
			m_aks_mtx.unlock_shared();
			mkdir(file_path.data(),S_IRWXU|S_IRWXG|S_IRWXO);
			file_path +=  "/" + msg->file_id;
			msg->file_fd = open(file_path.data(),O_WRONLY | O_CREAT ,0777);
			msg->offset = 0;
			if(msg->file_fd < 0)
			{
				LOG(ERROR)<<"open file:"<<file_path<<" error!"<<errno<<endl;
				return -1;
			}
		}

		uint64_t handle_size;
		if(msg->total_size - msg->offset > MAX_HANDLE_SIZE)//计算这次要处理的文件大小
		{
			handle_size = MAX_HANDLE_SIZE;
		}
		else
		{
			handle_size = msg->total_size - msg->offset;
			msg->done = true;
		}
		
		//开始接收文件
		char buffer[TCP_PACKAGE_SIZE];
		int ret;
		uint64_t recv_size = 0;
		if(msg->total_size > MAX_CACHE_SIZE)
		{
			char buffer[TCP_PACKAGE_SIZE];
			int ret;
			uint64_t recv_size = 0;
			while (recv_size < handle_size)
			{
				if(recv_size+TCP_PACKAGE_SIZE>handle_size)
				{
					ret = recv(msg->fd,buffer,handle_size-recv_size,0);
				}
				else
				{
					ret = recv(msg->fd,buffer,TCP_PACKAGE_SIZE,0);
				}
				if(ret==0)
				{
					LOG(ERROR)<<"recv file from fd:"<<msg->fd<<" interrupt!"<<endl;
					close(msg->file_fd);
					return -1;
				}
				else if(ret < 0)
				{
					if(errno == EAGAIN || errno == EWOULDBLOCK)//数据全部读取完毕
					{
						continue;//如果msg读到一半缓冲区没数据了，继续读
					}
					LOG(ERROR)<<"recv file from fd:"<<msg->fd<<" error!"<<errno<<endl;
					close(msg->file_fd);
					return -1;
				}
				else
				{
					write(msg->file_fd,buffer,ret);
					recv_size+=ret;
				}
			}
			msg->offset += handle_size;
			if(msg->done)
			{
				
				close(msg->file_fd);
				Errors err = m_model->AddFile(msg->file_id,m_machine_id,"main");
				LOG(INFO) << "recv file:" << msg->file_id<<" from fd:"<< msg->fd  <<" ok"<< endl;
				m_aks_mtx.lock_shared();
				if(need_back_up)
				{
					AddToBackup(m_aks[msg->fd],msg->file_id,msg->total_size);
				}
				m_aks_mtx.unlock_shared();
			}
			return handle_size;
		}
	}
	else
	{

		LOG(INFO) << "fd:"<< msg->fd << " begin to send file:" << msg->file_id << endl;
		msg->begin = false;
		msg->done = true;
		msg->offset = 0;
		int ret;
		uint64_t recv_size = 0;
		char  buffer[MAX_CACHE_SIZE];
		while (recv_size < msg->total_size)
		{
			ret = recv(msg->fd,buffer+msg->offset,msg->total_size-recv_size,0);
			if(ret==0)
			{
				LOG(ERROR)<<"recv file from fd:"<<msg->fd<<" interrupt!"<<endl;
				close(msg->file_fd);
				return -1;
			}
			else if(ret < 0)
			{
				if(errno == EAGAIN || errno == EWOULDBLOCK)//数据全部读取完毕
				{
					continue;//如果msg读到一半缓冲区没数据了，继续读
				}
				LOG(ERROR)<<"recv file from fd:"<<msg->fd<<" error!"<<errno<<endl;
				close(msg->file_fd);
				return -1;
			}
			else
			{
				msg->offset += ret; 
				recv_size += ret;
			}
		}
		leveldb::Slice slice(buffer,msg->total_size);
		leveldb::Status status = m_db->Put(leveldb::WriteOptions(),msg->file_id,slice);
		if(!status.ok())
		{
			LOG(ERROR)<<"write error"<<status.ToString()<<endl;
			return -1;
		}
		else
		{
			Errors err = m_model->AddFile(msg->file_id,m_machine_id,"main");
			if(err != Errors::Success)
			{
				LOG(ERROR)<<"write error,error type:"<<(int)err<<endl;
			}
			else if(need_back_up)
			{
				AddToBackup(msg->ak,msg->file_id,msg->total_size);
			}
		}
		LOG(INFO) << "recv file:" << msg->file_id<<" from fd:"<< msg->fd  <<" ok"<< endl;
		return msg->total_size;	
	}   
	return -1;
}
int64_t Worker::GetFile(MsgDesc * msg)
{
	if(msg->total_size > MAX_CACHE_SIZE)
	{
		if(msg->begin)//如果是刚开始
		{
			LOG(INFO) << "fd:" <<msg->file_id <<" begin to get file:"<<msg->file_id<< endl;
			Msg result;
			ResultResponse * res = (ResultResponse*)result.data;
			result.type = Types::GetFileResponse;
			m_aks_mtx.lock_shared();
			string file_path = "./"+m_aks[msg->fd] + "/" +msg->file_id;
			m_aks_mtx.unlock_shared();
			msg->file_fd = open(file_path.data(),O_RDONLY);
			if(msg->file_fd < 0)
			{
				LOG(ERROR)<<"open file:"<<file_path<<" error!"<<errno<<endl;
				res->err = Errors::OpenFileError;
				sprintf(res->msg,"errno:%d",errno);
				send(msg->fd,&result,sizeof(Msg),0);
				return -1;
			}
			res->err = Errors::Success;
			send(msg->fd,&result,sizeof(Msg),0);
			struct stat statbuf;
			stat(file_path.data(),&statbuf);
			msg->total_size=statbuf.st_size;
		}
		uint64_t handle_size;
		if(msg->total_size - msg->offset > MAX_HANDLE_SIZE)//计算这次要处理的文件大小
		{
			handle_size = MAX_HANDLE_SIZE;
		}
		else
		{
			handle_size = msg->total_size - msg->offset;
			msg->done = true;
		}
		off64_t offset = msg->offset;
		if(sendfile64(msg->fd,msg->file_fd,&offset,handle_size)<0)
		{
			LOG(ERROR)<<"sendfile64 return with value < 0,errno:"<<errno<<endl;
			return -1;
		}
		if(msg->done)
		{
			close(msg->file_fd);
			LOG(INFO) << "fd:" <<msg->file_id <<" get file:"<<msg->file_id<<" ok"<< endl;
		}
		return handle_size;
	}
	else
	{
		
		LOG(INFO) << "fd:" <<msg->file_id <<" begin to get file:"<<msg->file_id<< endl;
		Msg result;
		ResultResponse * res = (ResultResponse*)result.data;
		result.type = Types::GetFileResponse;
		res->err = Errors::Success;
		string data;
		leveldb::Status status = m_db->Get(leveldb::ReadOptions(),msg->file_id,&data);
		if(!status.ok())
		{
			res->err = Errors::OpenFileError;
			strcpy(res->msg,"read file error");
			LOG(ERROR)<<"read_file_error"<<status.ToString()<<endl;
			send(msg->fd,&result,sizeof(Msg),0);
			return -1;
		}
		send(msg->fd,&result,sizeof(Msg),0);

		if(send(msg->fd,data.data(),data.size(),0)<0)
		{
			LOG(ERROR)<<"sendfile return with value < 0,errno:"<<errno<<endl;
			return -1;
		}
		msg->done = true;
		LOG(INFO) << "fd:" <<msg->file_id <<" get file:"<<msg->file_id<<" ok"<< endl;
		return data.size();
	}
	
}

 IpPort Worker::GetBackupServer()
 {
    Msg msg, result;
    msg.type = Types::GetBackupServerRequest;
    msg.id = g_msg_id++;
	GetBackupServerReq * req = (GetBackupServerReq *)msg.data;
	strcpy(req->machine_id,m_machine_id.data());
    if (RecvWithRetry(msg, (sockaddr*)&m_registry_addr, result, Types::GetBackupServerResponse) == 0)
    {
        IpPort* ip = (IpPort*)result.data;
        return IpPort(ip->ip,ip->port);
    }
    else
    {
       LOG(ERROR) << "get backup server fail!" << endl;
       return IpPort(0,0);
    }
 }

void Worker::AddToBackup(string ak,string file_id,uint64_t file_size)
{
	BackupDesc * desc = new BackupDesc(file_id,ak,file_size);
	m_backup_q.enqueue(desc);
	sem_post(&m_backup_sem);
}
void Worker::BackupThread()
{
	
	int fd = 0;
	
	while(true)
	{
		if(fd == 0)
		{	
			fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

			IpPort file_server_ip = GetBackupServer();
			if(file_server_ip.ip == 0)
			{
				LOG(ERROR)<<"get backup server error!"<<endl;
				return;
			}
			LOG(INFO) << "backup server:" << IpToDot(file_server_ip.ip) << ":" << file_server_ip.port << endl;
			struct sockaddr_in backup_server_addr;
			backup_server_addr.sin_family = AF_INET;
			backup_server_addr.sin_port = htons(file_server_ip.port);
			backup_server_addr.sin_addr.s_addr = htonl(file_server_ip.ip);
			LOG(INFO) << "connecting to backup server..." << endl;
			if (fd <0)
			{
				LOG(ERROR)<<"connect backup server failed,invalid socket !"<<errno<<endl;
				return;
			}
			if (connect(fd, (sockaddr*)&backup_server_addr, sizeof(backup_server_addr)) < 0)
			{
				LOG(ERROR)<<"connect backup server error !"<<errno<<endl;
				return;
			}
			LOG(INFO)<<"connect to backup server success!"<<endl;
		}
		sem_wait(&m_backup_sem);
		BackupDesc * desc;
		if(m_backup_q.try_dequeue(desc))
		{
			string file_path = "./"+desc->ak+"/"+desc->file_id;
			int file_fd = open(file_path.c_str(),O_RDONLY);
			if(file_fd<0)
			{
				LOG(ERROR)<<"Open file failed! errno:"<<errno<<endl;
				close(file_fd);
				continue;
			}
			
		
			
			Msg msg;
			msg.type = Types::AddBackupFileRequest;
			msg.id = g_msg_id++;
			AddBackupFileReq* file_req = (AddBackupFileReq*)msg.data;
			strcpy(file_req->file_id, desc->file_id.data());
			strcpy(file_req->ak,desc->ak.data());
			file_req->file_size = desc->file_size;

			int ret = send(fd, &msg,sizeof(msg), 0);
			if(ret<0)
			{
				LOG(ERROR)<<"send req to file server error!"<<errno<<endl;
				close(file_fd);
				close(fd);
				fd = 0;
				return;
			}

			//上传文件
		
			ret = sendfile64(fd,file_fd,nullptr,desc->file_size);
			if(ret<0)
			{
				LOG(ERROR)<<"send file to server error!"<<errno<<endl;
				close(file_fd);
				close(fd);
				fd = 0;
				return;
			}
			close(file_fd);
		}
	}
}
void Worker::RemoveFd(int fd)
{
	LOG(INFO)<<"fd:"<<fd<<" disconnected"<<endl;
	m_aks_mtx.lock();
	m_aks[fd] = "";
	m_aks_mtx.unlock();
	g_tcp_num--;
	close(fd);
}
void Worker::ResetFd(int fd)
{
	epoll_event event;
    event.data.fd=fd;
    event.events=EPOLLIN|EPOLLET|EPOLLONESHOT;
    epoll_ctl(m_epoll_fd,EPOLL_CTL_MOD,fd,&event);
}
