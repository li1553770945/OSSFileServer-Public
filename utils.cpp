#include "utils.h"
#include <cerrno>

atomic_ullong g_msg_id(0);

unsigned int IpToInt(const char* str_ip)
{
    in_addr addr;
    unsigned int int_ip;
    if (inet_aton(str_ip, &addr))
    {
        int_ip = ntohl(addr.s_addr);
    }
    return int_ip;
}

string IpToDot(unsigned int n_ip) {
    in_addr addr;
    addr.s_addr = htonl(n_ip);
    string strip = inet_ntoa(addr);
    if (!strip.empty()) {
        return strip;
    }
    return NULL;
}
string GenerateRandomString(int length)
{
    string result;
    for (int i = 1; i <= length; i++)
    {
        int x = random() % 62;
        if (x < 10)
        {
            result += x + '0';
        }
        else if (x < 36)
        {
            result += x - 10 + 'a';
        }
        else
        {
            result += x - 36 + 'A';
        }
    }
    return result;
}

int RecvWithRetry(Msg& msg, sockaddr* server_addr, Msg& result, Types expect_types, int max_retry_time, int time_out_milli)
{
    static int fd = socket(AF_INET, SOCK_DGRAM, 0);
    struct timeval tv;
    tv.tv_sec = 1;
    tv.tv_usec = 0;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv)) < 0)
    {
        cout << "set timeout fail! errno:" << errno << endl;
    }
    for (int i = 1; i <= max_retry_time; i++)
    {
        int ret = sendto(fd, &msg, sizeof(msg), 0, (sockaddr*)server_addr, sizeof(sockaddr_in));
        if (ret > 0)
        {
            int count = recvfrom(fd, &result, sizeof(result), 0, 0, 0);  //recvfrom是拥塞函数，没有数据就一直拥塞
            if (count > 0 && result.id == msg.id && result.type == expect_types)
            {
                return 0;
            }
        }
        else
        {
            cout << "senddto fail! errno:" << errno << endl;
        }
    }
    return -1;
}