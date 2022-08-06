#include "model.h"
#include <sys/time.h>
#include <thread>
#include <string.h>
#include "easylogging++.h"

const int MAX_EXECER_NUM = 16;
const int SQL_TIME_WAIT = 7200 - 1200;//加一个偏移量确定不出问题


int Model::file_num = 0;
moodycamel::ConcurrentQueue <string> Model::m_sync_sqls;
MySqlPool* Model::m_pool;
sem_t Model::m_sqls_sem;

Model::Model(string host, string user, string pwd, string dbname, int port)
{
	m_pool = new MySqlPool(host, user, pwd, dbname, port);
	m_pool->Connect();
	sem_init(&m_sqls_sem, 0, 0);
	for (int i = 1; i <= MAX_EXECER_NUM; i++)
	{
		thread t(Execer);
		t.detach();
	}
}

void Model::Execer()
{
	MYSQL* mysql = m_pool->GetConnection();
	timeval start_tv,current_tv;
	gettimeofday(&start_tv,NULL);
	while (true)
	{
		gettimeofday(&current_tv,NULL);
		if(current_tv.tv_sec - start_tv.tv_sec > SQL_TIME_WAIT)
		{
			m_pool->RecoverConnection(mysql);
			mysql = m_pool->GetConnection();
		}
		sem_wait(&m_sqls_sem);
		
		string sql;
		if(!m_sync_sqls.try_dequeue(sql))
		{
			continue;;
		}
		mysql_query(mysql, sql.data());
		if(strlen(mysql_error(mysql))!=0)
		{
			LOG(ERROR)<<"mysql run error:"<<mysql_error(mysql)<<endl;
		}
	}
	m_pool ->RecoverConnection(mysql);
}
void Model::AddSqls(string sql)
{
	m_sync_sqls.enqueue(sql);
	sem_post(&m_sqls_sem);
}
Errors Model::Login(string ak,string sk)
{
	MYSQL* mysql = m_pool->GetConnection();
	char query[200];
	sprintf(query, "SELECT ak FROM user WHERE ak='%s' AND sk = '%s';",ak.data(),sk.data());
	mysql_query(mysql, query);
	MYSQL_RES* sql_res = mysql_store_result(mysql);
	if (mysql_num_rows(sql_res) == 0)
	{
		mysql_free_result(sql_res);
		return Errors::AuthFail;
	}
	m_pool->RecoverConnection(mysql);
	return Errors::Success;
}
string Model::GetAk(string token)
{
	return token;
}
string Model::GetToken(string access_key, string secret_key)
{
	return access_key;
}
Errors  Model::AddFile(string file_id, string machine_id,string type)
{
	static int cnt = 0;
	char query[200];
	sprintf(query, "INSERT INTO file_store(file_id,server_id,type) VALUES('%s',%s,'%s');",file_id.data(),machine_id.data(),type.data());
	AddSqls(query);
	return Errors::Success;
}
Errors Model::DeleteFile(string access_key, string file_id)
{
	char query[100];
	sprintf(query, "UPDATE file_meta SET has_delete = 1  WHERE file_id = '%s' AND ak = '%s';", file_id.data(), access_key.data());
	AddSqls(query);
	return Errors::Success;
}
Errors Model::GetFileStoredServer(string access_key, string file_id, vector <IpPort> &result)
{
	MYSQL* mysql = m_pool->GetConnection();
	char query[200];
	sprintf(query, "SELECT file_size FROM file_meta WHERE file_id='%s' AND ak = '%s' AND has_delete = 0;", file_id.data(), access_key.data());;
	mysql_query(mysql, query);
	MYSQL_RES* res = mysql_store_result(mysql);
	if (mysql_num_rows(res) == 0)
	{
		mysql_free_result(res);
		return Errors::FileNotExist;
	}
	


	sprintf(query, "SELECT server_id,type FROM file_store WHERE file_id='%s';", file_id.data());
	mysql_query(mysql, query);
	res = mysql_store_result(mysql);
	if (mysql_num_rows(res) == 0)
	{
		mysql_free_result(res);
		return Errors::FileStoreNotExist;
	}
	IpPort main;
	result.emplace_back(main);
	MYSQL_ROW row;
	while (row = mysql_fetch_row(res)) {

		sprintf(query, "SELECT addr,port FROM server WHERE server_id = '%s';", row[0]);
	
		mysql_query(mysql, query);
		MYSQL_RES* res_temp = mysql_store_result(mysql);
		MYSQL_ROW row_temp;
		while (row_temp = mysql_fetch_row(res_temp))
		{
			
			if (strcmp(row[1],"main")==0)
			{
				result[0].ip = IpToInt(row_temp[0]);
				result[0].port = atoi(row_temp[1]);
			}
			else
			{
				IpPort ip_temp;
				ip_temp.ip = IpToInt(row_temp[0]);
				ip_temp.port = atoi(row_temp[1]);
				result.emplace_back(ip_temp);
			}
		}
		mysql_free_result(res_temp);
	
	}


	mysql_free_result(res);

	return Errors::Success;
}
Model::~Model()
{
	delete m_pool;
}



