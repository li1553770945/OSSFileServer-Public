#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include <iostream>   
#include <unistd.h>
#include <sys/statfs.h>
#include <limits.h>

using namespace std;  
namespace Observer{
typedef struct MEMPACKED         //定义一个mem occupy的结构体  
{  
    char name1[20];      //定义一个char类型的数组名name有20个元素  
    unsigned long MemTotal;  
    char name2[20];  
    unsigned long MemFree;  
    char name3[20];  
    unsigned long Buffers;  
    char name4[20];  
    unsigned long Cached;  
    char name5[20];  
    unsigned long SwapCached;  
}MEM_OCCUPY;  
  
  
typedef struct CPUPACKED         //定义一个cpu occupy的结构体  
{  
    char name[20];      //定义一个char类型的数组名name有20个元素  
    unsigned int user; //定义一个无符号的int类型的user  
    unsigned int nice; //定义一个无符号的int类型的nice  
    unsigned int system;//定义一个无符号的int类型的system  
    unsigned int idle; //定义一个无符号的int类型的idle  
    unsigned int lowait;  
    unsigned int irq;  
    unsigned int softirq;  
}CPU_OCCUPY;  
  
  
  
int GetCpuOccupy(CPU_OCCUPY *cpust) //对无类型get函数含有一个形参结构体类弄的指针O  
{  
    FILE *fd;  
    char buff[256];  
    CPU_OCCUPY *cpu_occupy;  
    cpu_occupy = cpust;  
      
    fd = fopen("/proc/stat", "r");  
    fgets(buff, sizeof(buff), fd);  
      
    sscanf(buff, "%s %u %u %u %u %u %u %u", cpu_occupy->name, &cpu_occupy->user, &cpu_occupy->nice, &cpu_occupy->system, &cpu_occupy->idle, &cpu_occupy->lowait, &cpu_occupy->irq, &cpu_occupy->softirq);  
      
      
    fclose(fd);  
      
    return 0;  
}  
  
  
int CalCpuOccupy(CPU_OCCUPY *o, CPU_OCCUPY *n)  
{  
    unsigned long od, nd;  
    double cpu_use = 0;  
      
    od = (unsigned long)(o->user + o->nice + o->system + o->idle + o->lowait + o->irq + o->softirq);//第一次(用户+优先级+系统+空闲)的时间再赋给od  
    nd = (unsigned long)(n->user + n->nice + n->system + n->idle + n->lowait + n->irq + n->softirq);//第二次(用户+优先级+系统+空闲)的时间再赋给od  
    double sum = nd - od;  
    double idle = n->idle - o->idle;  
    cpu_use = idle / sum;  
    idle = n->user + n->system + n->nice - o->user - o->system - o->nice;  
    cpu_use = idle / sum;  
    return cpu_use * 100; 
}  
pair<uint64_t,uint64_t> GetMemoryUse()
{
    static  MEM_OCCUPY mem_stat; 
    FILE *fd;  
    char buff[256];  
    MEM_OCCUPY *m = &mem_stat;  
    fd = fopen("/proc/meminfo", "r");   
    fgets(buff, sizeof(buff), fd);  
    sscanf(buff, "%s %lu ", m->name1, &m->MemTotal);  
    fgets(buff, sizeof(buff), fd);  
    sscanf(buff, "%s %lu ", m->name2, &m->MemFree);  
    fgets(buff, sizeof(buff), fd);  
    sscanf(buff, "%s %lu ", m->name3, &m->Buffers);  
    fgets(buff, sizeof(buff), fd);  
    sscanf(buff, "%s %lu ", m->name4, &m->Cached);  
    fgets(buff, sizeof(buff), fd);   
    sscanf(buff, "%s %lu", m->name5, &m->SwapCached);  
    fclose(fd);     //关闭文件fd  

    return make_pair(m->MemTotal,m->MemFree);
}
int GetCpuUse()
{
    CPU_OCCUPY cpu_stat1;  
    CPU_OCCUPY cpu_stat2;
    GetCpuOccupy((CPU_OCCUPY *)&cpu_stat1);  
    
    usleep(100000);
    
    //第二次获取cpu使用情况  
    GetCpuOccupy((CPU_OCCUPY *)&cpu_stat2);  

    return CalCpuOccupy((CPU_OCCUPY *)&cpu_stat1, (CPU_OCCUPY *)&cpu_stat2);  
}

string GetCurPath()
{
    char *p = NULL;
    const int len = 256;
    char arr_tmp[len] = {0};
    int n = readlink("/proc/self/exe", arr_tmp, len);
    if ((p = strrchr(arr_tmp,'/')) != NULL)
    {
        *p = '\0';
    }
    else
    {
       return "";
    }

    return arr_tmp;
}
pair<uint64_t,uint64_t> GetDiskUse()
{
    string exec_str    = GetCurPath();
	struct statfs diskInfo;
	statfs(exec_str.c_str(), &diskInfo);
	unsigned long long block_size = diskInfo.f_bsize;	//每个block里包含的字节数
	unsigned long long total_size = block_size * diskInfo.f_blocks; 	//总的字节数，f_blocks为block的数目
	unsigned long long available_disk = diskInfo.f_bavail * block_size; 	//可用空间大小
	return make_pair(total_size,available_disk);
}
}