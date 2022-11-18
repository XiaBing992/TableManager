#ifndef TABLEMANAGER_H
#define TABLEMANAGER_H
#include <stdio.h>
#include <fstream>
#include<vector>
#include<shared_mutex>
#include<unordered_map>
#include<map>
#include "Record.h"
#include "BPlusTree.h"
#include "Logger.h"
#include "Buffer.h"

#define FILE_NAME "./Table/table.data"
#define CONFIG_FILE "./Table/table.config"
#define BPLUSTREE_FILE "./Table/BPlusTree.data"
#define ATTRIBUTE_NUM 100
#define RECORD_SIZE sizeof(Record)
#define INIT_RECORD_SIZE 2000000


//系统默认支持阶数为奇数
#define BPLUSTREE_ORDER 19


//表管理类
class TableManager
{
private:
    TableManager();
    //文件描述符
    fstream m_gFd[2];
    //记录数
    int m_nRecordNum;
    //属性数
    int m_nAttributeNum;


    //缓冲区
    Buffer m_cBuffer;

    //互斥访问记录数
    shared_mutex m_smRecordNumMutex;

    //记录建立了索引的索引列
    unordered_map<int,BPlusTree*> m_umIndexedCol;

    //日志
    Logger& logger=Logger::getInstance();

    //随机初始化表
    void initTableByRand();
    //序列化
    void serializeBPlusTree();
    //反序列化
    void deSerializeBPlusTree();
    //通过索引查找
    map<int,Record> searchByIndex(const int col,const int low,const int high);
    //直接搜索
    map<int,Record> searchDirect(const int col,const int low,const int high);

    //将Table一定范围的数据插入b+树[low，high)（用于多线程）
    void insertBPlusTreeByTableRange(BPlusTree* bPlusTree,const int col,const int low,const int high,int start_pos);
    //将插入缓冲区写入文件
    void flushInsertBuffer();
    //使用多线程为缓冲区数据创建索引
    void createIndexForBuffer(BPlusTree* bPlusTree,const int col,const int data_num,int start_pos);

public:
    //禁止拷贝
    TableManager(TableManager& tableManager)=delete;
    TableManager& operator=(TableManager& tableManager)=delete;


    ~TableManager()
    {
        //将更新的数据写入
        flushInsertBuffer();
        serializeBPlusTree();
 
        m_gFd[0].close();
        m_gFd[1].close();
    }
    //获得单例
    static TableManager& getTableManagerInstance();

    //根据记录下标得到记录
    Record getRecordByIndex(int index);

    //追加数据
    void append(Record r);

    map<int,Record> search(const int col,const int low,const int high);
    //为某个属性创建索引
    void createIndex(const int col);
    //删除某个属性索引
    void deleteIndex(const int col);
};
// TableManager* TableManager::my_table_manager;

#endif