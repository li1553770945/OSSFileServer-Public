all:
	g++ -DELPP_THREAD_SAFE database.cpp easylogging++.cpp file_server.cpp main.cpp model.cpp observer.cpp utils.cpp worker.cpp -std=c++17 -I/usr/include/mysql -L/usr/lib64/mysql -L/usr/local/lib -lleveldb -lpthread -lmysqlclient  -lz -lm -ldl -lssl -lcrypto -o build/FileServer