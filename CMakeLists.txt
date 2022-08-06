cmake_minimum_required(VERSION 3.24) 
set(CMAKE_SYSTEM_NAME Linux)
set(CMAKE_CXX_COMPILER "/usr/bin/g++")

set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2 -g")
add_definitions(-DELPP_THREAD_SAFE)
# set the project name 
project(FileServer) 
add_link_options(-I/usr/include/mysql -L/usr/lib64/mysql  -L/usr/local/lib -lmysqlclient -lpthread -lleveldb -lz -lm -ldl -lssl -lcrypto )
set(CMAKE_LD_FLAGS "-lpthread -lleveldb")
# add the executable 

aux_source_directory(. DIR_SRCS)

add_executable(FileServer ${DIR_SRCS}) 