#pragma once
#include <iostream>
#include <list>
#include <semaphore.h>
#include <queue>
#include "utils.h"
#include <mysql/mysql.h>
#include "concurrentqueue.h"

using namespace std;

class MySqlPool {
	static sem_t m_need_conn_sem,m_conn_sem;
	static string m_host, m_user, m_pwd, m_dbname;
	static int m_port;
	static moodycamel::ConcurrentQueue <MYSQL*> m_conns;
public:
	static MYSQL* GetConnection();
	static void ConnectionGenerater();
	static void RecoverConnection(MYSQL* connection);
	MySqlPool(string host, string user, string pwd, string dbname,int port);
	static void Connect();
	~MySqlPool();
};





