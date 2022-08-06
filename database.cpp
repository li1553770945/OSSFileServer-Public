#include "database.h"
#include <stdlib.h>
#include <thread>
#include <semaphore.h>
#include <mutex>
#include <sys/time.h>
#include "easylogging++.h"

const int MYSQL_WAIT_TIME = 7200;
const int POOL_MAX_CONNS = 1024;

sem_t MySqlPool::m_need_conn_sem, MySqlPool::m_conn_sem;
int MySqlPool::m_port;
string MySqlPool::m_host, MySqlPool::m_user, MySqlPool::m_pwd, MySqlPool::m_dbname;
moodycamel::ConcurrentQueue <MYSQL*> MySqlPool::m_conns;

MySqlPool::MySqlPool(string host, string user, string pwd, string dbname,int port)
{
	m_host = host;
	m_user = user;
	m_pwd = pwd;
	m_dbname = dbname;
	m_port = port;
	sem_init(&m_need_conn_sem, 0, POOL_MAX_CONNS);
	sem_init(&m_conn_sem, 0, 0);

	
}

void MySqlPool::Connect()
{
	thread t(ConnectionGenerater);
	t.detach();
}

MYSQL* MySqlPool::GetConnection()
{
	sem_wait(&m_conn_sem);

	MYSQL* conn;
	while(!m_conns.try_dequeue(conn))
	{
		;
	}
	mysql_ping(conn);
	return conn;
}
void MySqlPool::ConnectionGenerater()
{

	while (true)
	{
		sem_wait(&m_need_conn_sem);
		MYSQL* connection = mysql_init(NULL);
		char value = 1;
 		mysql_options(connection, MYSQL_OPT_RECONNECT, (char *)&value);
		connection = mysql_real_connect(connection, m_host.c_str(),
			m_user.c_str(), m_pwd.c_str(), m_dbname.c_str(), 3306, NULL, 0);
		if (connection == NULL)
		{
			LOG(ERROR) << "Connect databases failed!" << endl;
			sem_post(&m_need_conn_sem);
			continue;
		}
	
		m_conns.enqueue(connection);
		sem_post(&m_conn_sem);
	}
}
void MySqlPool::RecoverConnection(MYSQL* connection)
{
	if(connection)
	{
		
		m_conns.enqueue(connection);
		sem_post(&m_conn_sem);
	}
	
}

MySqlPool::~MySqlPool()
{
	MYSQL* conn;
	while (m_conns.try_dequeue(conn))
	{
		mysql_close(conn);
	}
	
}

