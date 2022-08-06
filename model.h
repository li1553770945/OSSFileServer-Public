#pragma once

#include <iostream>
#include <list>
#include <semaphore.h>
#include <queue>
#include "utils.h"
#include <mysql/mysql.h>
#include "database.h"
#include "concurrentqueue.h"

class Model {
private:
	static int file_num;
	static moodycamel::ConcurrentQueue <string> m_sync_sqls;
	static MySqlPool* m_pool;
	static sem_t m_sqls_sem;
private:
	static void Execer();//异步sql执行函数
	void AddSqls(string sqls);
public:


	Model(string host, string user, string pwd, string dbname, int port);

	
	Errors Login(string ak,string sk);
	string GetToken(string access_key, string secret_key);
	string GetAk(string token);
	Errors AddFile(string file_id, string machine_id,string type);
	Errors DeleteFile(string access_key, string file_id);
	Errors GetFileStoredServer(string access_key, string file_id, vector <IpPort>& result);
	~Model();

};