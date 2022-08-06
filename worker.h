#include <iostream>
#include "model.h"
#include "concurrentqueue.h"
#include <unordered_map>
#include <shared_mutex>
#include <leveldb/db.h>

using namespace std;

extern atomic_uint64_t g_tcp_num,g_action_num;


const int MESSAGE_LEN = sizeof(Msg);
const int MAX_EVENT_NUMBER = 10240;
const int MAX_WORK_THREAD = 32;
const int MAX_HANDLE_SIZE = 1024*1024*16;//每次最多处理16MB
const int TCP_PACKAGE_SIZE = 1024*64;
const int BACKUP_THREAD = 2;
const int MAX_CACHE_SIZE = 8192;
const int MAX_WRITER_THREAD = 4;

class MsgDesc{
public:
	Types type;
	string file_id;
	string ak;
	int fd;
	int file_fd;
	bool begin,done;
	uint64_t offset;
	uint64_t total_size;
	MsgDesc(int fd);
};

struct WriterDesc{
	int file_fd;
	string file_id;
	uint64_t file_size;
	string ak;
	char * buffer;
	int need_back_up;
	WriterDesc(int _file_fd = 0,uint64_t _size = 0,char * _buffer = nullptr,string _file_id = "",string _ak = "",int _need_back_up=1)
	{
		file_fd = _file_fd;
		file_size = _size;
		buffer = _buffer;
		file_id = _file_id;
		ak = _ak;
		need_back_up = _need_back_up;
	}
};


class BackupDesc{
public:
	string file_id,ak;
	uint64_t file_size;
	BackupDesc(string _file_id,string _ak,uint64_t _file_size)
	{
		file_id = _file_id;
		ak = _ak;
		file_size = _file_size;
	}
};

class Worker{
private:
	leveldb::DB* m_db;
	sockaddr_in m_registry_addr;
	int m_epoll_fd;
	string m_machine_id;
	Model * m_model;
	moodycamel::ConcurrentQueue<MsgDesc*> m_ready_fd_q;
	moodycamel::ConcurrentQueue<BackupDesc *> m_backup_q;
	moodycamel::ConcurrentQueue<WriterDesc> m_writer_q;
	sem_t m_ready_fd_sem,m_backup_sem,m_writer_sem;
	unordered_map <int,string> m_aks;
	shared_mutex m_aks_mtx;

private:
	

	void WorkThread();
	void BackupThread();
	int Login(int fd,Msg &msg);

	bool RecvOneMsg(MsgDesc * msg_desc,Msg & msg);//返回是否接收成功
	int64_t AddFile(MsgDesc * msg,bool _need_back_up);//返回实际处理的大小
	int64_t GetFile(MsgDesc * msg);//返回实际处理的大小
	IpPort GetBackupServer();
	void AddToBackup(string ak,string file_id,uint64_t file_size);
	bool CheckLogin(int fd);
	void RemoveFd(int fd);
	void ResetFd(int fd);
	void WriterThread();
public:
		
	Worker(sockaddr_in egistry_addr,int epoll_fd,Model * model,string machine_id,leveldb::DB* db);
	void Run();
};