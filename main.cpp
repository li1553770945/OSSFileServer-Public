#include "file_server.h"
#include "easylogging++.h"

INITIALIZE_EASYLOGGINGPP
int main(int argc,char *argv[])
{

	el::Configurations conf("./log.conf");
    el::Loggers::reconfigureAllLoggers(conf);
	FileServer file_server("9.135.35.137",14001,"127.0.0.1",12001, "9.135.135.245", 3306, "root", "Li5060520", "oss");
	file_server.Run();

	return 0;
}