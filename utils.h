#pragma once
#include "types.h"
#include <iostream>
#include <unistd.h>
#include <arpa/inet.h>
#include <atomic>
using namespace std;
class Msg {
public:
	Types type;
	unsigned long long id;
	unsigned len;
	char data[64];
};

#pragma pack(push) //保存对齐状态 
#pragma pack(1)//设定为1字节对齐 

class IpPort {
public:
	uint32_t ip;
	uint32_t port;
	IpPort(unsigned int _ip=0, unsigned int _port=0):ip(_ip),port(_port)
	{

	}
	bool operator == (const IpPort& rhs)const
	{
		return ip == rhs.ip && port == rhs.port;
	}
};

namespace std
{
    template<>
    struct hash<IpPort>
    {
        size_t operator() (const IpPort& s) const noexcept
        {
            return   s.ip + s.port;
        }
    }; 
}

//rigistry

struct ServerStatus{
	uint32_t cpu_rate;
	uint64_t total_memory;
	uint64_t free_memory;
	uint64_t total_disk;
	uint64_t free_disk;
	uint64_t tcp_num;
	uint64_t action_num;
};
struct AddServerReq{
	IpPort ip;
	ServerStatus status;
	
};
#pragma pack(pop)//恢复对齐状态
struct GetBackupServerReq{
	char machine_id[10];
};

//server 

struct LoginReq{
	char ak[16];
	char sk[16];
};

struct LoginRes{
	Errors err;
	char msg[32];
};

//添加文件
struct AddFileMetaReq {
	char file_name[32];
	unsigned long long file_size;
};

struct AddFileMetaRes {
	char file_id[32];
};

//查询文件
struct GetFileMetaReq {
	char file_id[32];
};

struct GetFileMetaRes {
	Errors err;
	int file_size;
	char file_name[32];
};

//查询文件存储服务器
struct GetFileStoreServerReq {
	char file_id[32];
};

struct GetFileStoreServerRes {
	int size;
	IpPort ip[4];
};

//删除文件
struct DeleteFileMetaReq {
	char file_id[32];
};

//file server

struct AddFileReq {
	char file_id[32];
	unsigned long long file_size;
};

struct GetFileReq{
	char file_id[32];
	uint64_t offset;
};

struct AddBackupFileReq{
	char file_id[32];
	char ak[16];
	unsigned long long file_size;
};

//all 
struct ResultResponse {
	Errors err;
	char msg[48];
};

extern atomic_ullong g_msg_id;
unsigned int IpToInt(const char* str_ip);
string IpToDot(unsigned int nIp);
string GenerateRandomString(int length);
int RecvWithRetry(Msg& msg, sockaddr* server_addr, Msg& result, Types expect_types, int max_retry_time = 5, int time_out_milli = 1000);
