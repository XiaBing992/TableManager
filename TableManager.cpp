#include <iostream>
#include <thread>
#include <future>
#include <string.h>
#include <fstream>
#include <algorithm>
#include "TableManager.h"

using namespace std;

//获得单例
TableManager &TableManager::getTableManagerInstance()
{
    static TableManager my_table_manager;

    return my_table_manager;
}

TableManager::TableManager()
{
    m_nRecordNum = 0;

    m_gFd[0].open(CONFIG_FILE, ios::binary | ios::in | ios::out);
    m_gFd[1].open(FILE_NAME, ios::binary | ios::in | ios::out);
    //打开异常
    if (!(m_gFd[0] && m_gFd[1]))
    {
        logger.writeLog("open file error.", Logger::LogType::ERROR);
        exit(-1);
    }

    //判断表是否为空
    m_gFd[1].seekg(0, m_gFd[1].end);
    int offset = m_gFd[1].tellg();
    if (offset == 0)
    {
        initTableByRand();
    }
    else
    {
        logger.writeLog("deSerialize...",Logger::LogType::INFO);
        //反序列化索引B+树
        deSerializeBPlusTree();
        // cout << "read." << endl;
        //从文件读取配置数据
        m_gFd[0].read(reinterpret_cast<char *>(&m_nRecordNum), sizeof(int));
        m_nAttributeNum = ATTRIBUTE_NUM;
        m_gFd[1].seekg(0, m_gFd[1].beg);

        //记录数比缓冲区小
        if (m_nRecordNum <= MAX_RECORD_BUFFER_SIZE)
        {
            m_gFd[1].read(reinterpret_cast<char *>(m_cBuffer.m_pTableBuffer), RECORD_SIZE * m_nRecordNum);
            m_cBuffer.m_nRecordStartPos= 0;
            m_cBuffer.m_nTableBufferSize = m_nRecordNum;
        }
        else
        {
            m_gFd[1].read(reinterpret_cast<char *>(m_cBuffer.m_pTableBuffer), RECORD_SIZE * MAX_RECORD_BUFFER_SIZE);
            m_cBuffer.m_nRecordStartPos =0;
            m_cBuffer.m_nTableBufferSize = MAX_RECORD_BUFFER_SIZE;
        }
        logger.writeLog("record_num:" + to_string(m_nRecordNum), Logger::LogType::DEBUG);
        logger.writeLog("deSerialize end...",Logger::LogType::INFO);
    }
}
//序列化
void TableManager::serializeBPlusTree()
{
    /*----------------序列化B+树----------------*/
    string file_path = "./Table/BPlusTree/BPlusTree";
    fstream bPlusTree_config(file_path + ".config", ios::out | ios::trunc);

    for (unordered_map<int, BPlusTree *>::iterator ite = m_umIndexedCol.begin(); ite != m_umIndexedCol.end(); ite++)
    {
        bPlusTree_config << ite->first << " ";
        ite->second->serialization(file_path + to_string(ite->first) + ".data");
    }
    bPlusTree_config.close();
}
//反序列化
void TableManager::deSerializeBPlusTree()
{
    //反序列化索引数据
    string file_path = "./Table/BPlusTree/BPlusTree";
    fstream bPlusTree_config(file_path + ".config");

    int col = -1;
    while (bPlusTree_config >> col)
    {
        // cout<<col<<endl;
        BPlusTree *bPlusTree = BPlusTree::deSerialization(file_path + to_string(col) + ".data");
        m_umIndexedCol[col] = bPlusTree;
    }
}

//随机初始化表
void TableManager::initTableByRand()
{
    logger.writeLog("initTableByindex...", Logger::LogType::INFO);
    srand(unsigned(time(0)));
    //缓冲区大小大于初始化记录大小
    if (MAX_RECORD_BUFFER_SIZE >= INIT_RECORD_SIZE)
    {
        const int row = INIT_RECORD_SIZE, col = ATTRIBUTE_NUM;
        int index = 0;
        for (int i = 0; i < row; i++)
        {
            Record record = Record();
            for (int j = 0; j < col; j++)
            {
                record.m_gAttributes[j]=i * col + j;
            }
            random_shuffle(record.m_gAttributes,record.m_gAttributes+ATTRIBUTE_NUM);
            m_cBuffer.m_pTableBuffer[index++] = record;
        }
        //随机打乱数据
        random_shuffle(m_cBuffer.m_pTableBuffer,m_cBuffer.m_pTableBuffer+MAX_RECORD_BUFFER_SIZE);

        this->m_nRecordNum = row;
        this->m_nAttributeNum = col;

        //缓冲区数据写入文件
        m_gFd[1].seekg(0, m_gFd[1].beg);
        m_gFd[1].write((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * INIT_RECORD_SIZE);

        return;
    }

    //分几次写入文件
    m_gFd[1].seekg(0, m_gFd[1].beg);
    int record_start_pos = 0;
    int record_end_pos = 0;

    while (record_end_pos + MAX_RECORD_BUFFER_SIZE < INIT_RECORD_SIZE)
    {
        record_start_pos = record_end_pos;
        record_end_pos += MAX_RECORD_BUFFER_SIZE;
        //生成数据
        for (int i = 0; i < MAX_RECORD_BUFFER_SIZE; i++)
        {
            // cout<<i<<endl;
            for (int j = 0; j < ATTRIBUTE_NUM; j++)
            {
                m_cBuffer.m_pTableBuffer[i].m_gAttributes[j] = (i + record_start_pos) * ATTRIBUTE_NUM + j;
            }
            random_shuffle(m_cBuffer.m_pTableBuffer[i].m_gAttributes,m_cBuffer.m_pTableBuffer[i].m_gAttributes+ATTRIBUTE_NUM);
        }
        //随机打乱数据
        random_shuffle(m_cBuffer.m_pTableBuffer,m_cBuffer.m_pTableBuffer+MAX_RECORD_BUFFER_SIZE);
        //写入数据
        if (!m_gFd[1].write((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * MAX_RECORD_BUFFER_SIZE))
        {
            logger.writeLog("init error.", Logger::LogType::ERROR);
        }
        //描述符后移
        m_gFd[1].seekg(sizeof(Record) * record_end_pos, m_gFd[1].beg);
    }

    //写最后一部分数据
    record_start_pos = record_end_pos;
    record_end_pos = INIT_RECORD_SIZE;
    int data_num = record_end_pos - record_start_pos;
    //生成数据
    for (int i = 0; i < data_num; i++)
    {
        // cout<<i<<endl;
        for (int j = 0; j < ATTRIBUTE_NUM; j++)
        {
            m_cBuffer.m_pTableBuffer[i].m_gAttributes[j] = (i + record_start_pos) * ATTRIBUTE_NUM + j;
        }
        random_shuffle(m_cBuffer.m_pTableBuffer[i].m_gAttributes,m_cBuffer.m_pTableBuffer[i].m_gAttributes+ATTRIBUTE_NUM);
    }
    //随机打乱数据
    random_shuffle(m_cBuffer.m_pTableBuffer,m_cBuffer.m_pTableBuffer+data_num);

    m_gFd[1].seekg(sizeof(Record) * record_start_pos, m_gFd[1].beg);
    m_gFd[1].write((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * data_num);

    m_cBuffer.m_nRecordStartPos=record_start_pos;
    m_cBuffer.m_nTableBufferSize=data_num;
    this->m_nRecordNum = INIT_RECORD_SIZE;
    this->m_nAttributeNum = ATTRIBUTE_NUM;

}
//追加数据
void TableManager::append(Record r)
{
    logger.writeLog("append record...",Logger::LogType::INFO);
    unique_lock<shared_mutex> buffer_lock(m_cBuffer.m_smInsertTableMutex);
    m_cBuffer.m_pInsertBuffer[m_cBuffer.m_nInsertBufferSize++] = r;
    buffer_lock.unlock();
    m_nRecordNum++;

    //处理建立了索引的列
    if (m_umIndexedCol.size() != 0)
    {
        logger.writeLog("create index for new record...",Logger::LogType::INFO);
        int key, value;
        for (unordered_map<int, BPlusTree *>::iterator ite = m_umIndexedCol.begin(); ite != m_umIndexedCol.end(); ite++)
        {
            key = r.m_gAttributes[ite->first];
            value = m_nRecordNum - 1;
            // b+树插入
            ite->second->insert(key, value);
        }
    }
    logger.writeLog("append record end",Logger::LogType::INFO);

}


//创建索引
void TableManager::createIndex(const int col)
{
    logger.writeLog("create Index of col " + to_string(col) + " ...", Logger::LogType::INFO);
    unordered_map<int, BPlusTree *>::iterator ite = m_umIndexedCol.find(col);
    //已有索引
    if (ite != m_umIndexedCol.end())
    {
        logger.writeLog("the index of " + to_string(col) + " is already exits.", Logger::LogType::ERROR);
        return;
    }
    unique_lock<shared_mutex> buffer_lock(m_cBuffer.m_smTableMutex);
    //新建立索引
    BPlusTree *bPlusTree = new BPlusTree(BPLUSTREE_ORDER);
    Record record;
    int key, value;

    //文件数据已经被全部读进来
    if (m_nRecordNum <= MAX_RECORD_BUFFER_SIZE)
    {
        //为缓冲区数据建立索引
        createIndexForBuffer(bPlusTree, col,m_nRecordNum, 0);
    }
    else
    {
        /*---------------------------读入数据到缓冲区--------------------------*/
        m_gFd[1].seekg(0, m_gFd[1].beg);
        int record_start_pos = 0;
        int record_end_pos = 0;
        // m_cBuffer.m_nTableBufferSize = MAX_RECORD_BUFFER_SIZE;
        while (record_end_pos + MAX_RECORD_BUFFER_SIZE < m_nRecordNum)
        {
            if(!m_gFd[1].read((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * MAX_RECORD_BUFFER_SIZE))
            {
                //cout<<"ttt"<<endl;
                exit(-1);
            }
            
            record_start_pos = record_end_pos;
            record_end_pos += MAX_RECORD_BUFFER_SIZE;
            //为缓冲区数据建立索引
            createIndexForBuffer(bPlusTree, col, MAX_RECORD_BUFFER_SIZE, record_start_pos);

            //描述符后移
            m_gFd[1].seekg(sizeof(Record) * record_end_pos, m_gFd[1].beg);
        }
        //读取文件最后部分
        record_start_pos = record_end_pos;
        record_end_pos = m_nRecordNum;
        int data_num = record_end_pos - record_start_pos;
        m_gFd[1].seekg(sizeof(Record) * record_start_pos, m_gFd[1].beg);
        m_gFd[1].read((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * data_num);

        m_cBuffer.m_nRecordStartPos=record_start_pos;
        m_cBuffer.m_nTableBufferSize=data_num;
        //为缓冲区创建索引
        createIndexForBuffer(bPlusTree, col,data_num, record_start_pos);
    }

    //插入索引
    m_umIndexedCol.insert({col, bPlusTree});
    logger.writeLog("create Index of col " + to_string(col) + " end.", Logger::LogType::INFO);

    return;
}
void TableManager::createIndexForBuffer(BPlusTree *bPlusTree,const int col, const int data_num, int start_pos)
{
    /*---------------B+树多线程的插入---------------*/
    //得到当前机器线程最佳数
    int best_thread_num = thread::hardware_concurrency();
    vector<thread> vThreads;
    int batch = data_num / best_thread_num;
    for (int i = 0; i < best_thread_num; i++)
    {
        if (i != best_thread_num - 1 && batch != 0)
        {
            vThreads.push_back(thread(&TableManager::insertBPlusTreeByTableRange, this, bPlusTree, col, i * batch, (i + 1) * batch, start_pos));
            continue;
        }
        else
        {
            vThreads.push_back(thread(&TableManager::insertBPlusTreeByTableRange, this, bPlusTree, col, i * batch, data_num, start_pos));
            break;
        }
    }
    //等待线程结束
    for (int i = 0; i < best_thread_num; i++)
    {
        vThreads[i].join();
    }
}
//将Table一定范围的数据插入b+树[low，high)（用于多线程）
void TableManager::insertBPlusTreeByTableRange(BPlusTree *bPlusTree,const int col, const int low, const int high, int start_pos)
{
    //logger.writeLog("insertBPlusTreeByTableRange: "+to_string(start_pos+low)+" "+to_string(start_pos+high),Logger::LogType::DEBUG);
    Record record;
    int key, value;
    for (int i = low; i < high; i++)
    {
        record = m_cBuffer.m_pTableBuffer[i];
        //属性值作为key
        key = record.m_gAttributes[col];
        //地址作为value
        value = i + start_pos;
        bPlusTree->insert(key, value);
    }
}
//查询(未建立索引直接查找，建立了索引使用B+树查找)
map<int, Record> TableManager::search(const int col, const int low, const int high)
{
    logger.writeLog("search col: "+to_string(col)+" range:"+to_string(low)+" "+to_string(high), Logger::LogType::INFO);
    map<int, Record> res;
    if (low < 0 || low > high)
    {
        logger.writeLog("the params of search error.", Logger::LogType::ERROR);
        return res;
    }
    unordered_map<int, BPlusTree *>::iterator ite = m_umIndexedCol.find(col);
    if (ite != m_umIndexedCol.end())
    {
        logger.writeLog("searchByIndex...", Logger::LogType::INFO);
        //使用索引搜索
        return searchByIndex(col, low, high);
    }
    logger.writeLog("search directly...", Logger::LogType::INFO);
    //直接搜索
    return searchDirect(col, low, high);
}
//直接搜索
map<int, Record> TableManager::searchDirect(const int col, const int low, const int high)
{
    //刷新缓冲区
    flushInsertBuffer();

    unique_lock<shared_mutex> buffer_lock(m_cBuffer.m_smTableMutex);
    map<int, Record> res;
    //文件数据已经被全部读进来
    if (m_nRecordNum <= MAX_RECORD_BUFFER_SIZE)
    {
        int value;
        for (int i = 0; i < m_nRecordNum; i++)
        {
            value = m_cBuffer.m_pTableBuffer[i].m_gAttributes[col];
            if (value >= low && value <= high)
            {
                res[i] = m_cBuffer.m_pTableBuffer[i];
            }
        }

        return res;
    }

    //从文件头开始读
    m_gFd[1].seekg(0, m_gFd[1].beg);
    int record_start_pos = 0;
    int record_end_pos = 0;
    while (record_end_pos + MAX_RECORD_BUFFER_SIZE < m_nRecordNum)
    {
        if(!m_gFd[1].read((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * MAX_RECORD_BUFFER_SIZE))
        {
            logger.writeLog("read error.",Logger::LogType::ERROR);
        }
        record_start_pos = record_end_pos;
        record_end_pos += MAX_RECORD_BUFFER_SIZE;
        //在Buffer里搜索数据
        int value;
        for (int i = 0; i < MAX_RECORD_BUFFER_SIZE; i++)
        {
            value = m_cBuffer.m_pTableBuffer[i].m_gAttributes[col];
            //logger.writeLog(to_string(value),Logger::LogType::DEBUG);
            if (value >= low && value <= high)
            {
                // i+record_start_pos为记录真实行数
                //logger.writeLog(to_string(i+record_start_pos),Logger::LogType::DEBUG);
                res[i + record_start_pos] = m_cBuffer.m_pTableBuffer[i];
            }
        }

        //描述符后移
        m_gFd[1].seekg(sizeof(Record) * record_end_pos, m_gFd[1].beg);
    }
    //读取文件最后部分
    record_start_pos = record_end_pos;
    record_end_pos = m_nRecordNum;
    int data_num = record_end_pos - record_start_pos;
    m_gFd[1].seekg(sizeof(Record) * record_start_pos, m_gFd[1].beg);
    m_gFd[1].read((char *)m_cBuffer.m_pTableBuffer, sizeof(Record) * data_num);

    //在Buffer里搜索数据
    int value;
    for (int i = 0; i < data_num; i++)
    {
        value = m_cBuffer.m_pTableBuffer[i].m_gAttributes[col];
        if (value >= low && value <= high)
        {
            // i+m_cBuffer.m_nStartPos为记录真实行数
            res[i + record_start_pos] = m_cBuffer.m_pTableBuffer[i];
        }
    }
    m_cBuffer.m_nRecordStartPos=record_start_pos;
    m_cBuffer.m_nTableBufferSize=data_num;

    return res;
}
//将插入缓冲区写入文件
void TableManager::flushInsertBuffer()
{
    unique_lock<shared_mutex> buffer_lock(m_cBuffer.m_smInsertTableMutex);
    //写配置文件
    m_gFd[0].seekg(0, m_gFd[0].end);
    m_gFd[0].write((char *)&m_nRecordNum, sizeof(int));

    //写数据
    m_gFd[1].seekg(0, m_gFd[1].end);
    m_gFd[1].write((char *)m_cBuffer.m_pInsertBuffer, sizeof(Record) * m_cBuffer.m_nInsertBufferSize);

    m_cBuffer.m_nInsertBufferSize=0;
}
//通过索引查找
map<int, Record> TableManager::searchByIndex(const int col, const int low, const int high)
{
    /*----------使用多线程进行搜索---------------*/
    BPlusTree *bPlusTree = m_umIndexedCol[col];
    //cout<<bPlusTree->getDataNum()<<endl;
    //用于接收B+树搜索结果
    vector<int> all_address;
    //得到当前机器线程最佳数
    int best_thread_num = thread::hardware_concurrency();
    //每一个线程的任务量
    int batch = (high - low) / best_thread_num;
    //用于得到每一个线程的计算结果
    promise<vector<int>> promise_ret[best_thread_num];
    future<vector<int>> future_ret[best_thread_num];
    //存储线程
    vector<thread> vThreads;

    for (int i = 0; i < best_thread_num; i++)
    {
        //绑定未来返回值
        future_ret[i] = promise_ret[i].get_future();

        if (i != best_thread_num - 1 && batch != 0)
        {
            vThreads.push_back(thread(&BPlusTree::searchRange, bPlusTree, i * batch + low, (i + 1) * batch + low - 1, &promise_ret[i]));
        }
        else
        {
            vThreads.push_back(thread(&BPlusTree::searchRange, bPlusTree, i * batch + low, high, &promise_ret[i]));
            break;
        }
    }
    //等待线程结束
    for (int i = 0; i < vThreads.size(); i++)
    {
        vThreads[i].join();
        //收集各线程的结果
        vector<int> thread_ret = future_ret[i].get();
        all_address.insert(all_address.end(), thread_ret.begin(), thread_ret.end());
    }

    //得到地址对应的记录
    sort(all_address.begin(),all_address.end());
    map<int, Record> res;
    for (vector<int>::iterator ite = all_address.begin(); ite != all_address.end(); ite++)
    {
        res[*ite] = getRecordByIndex(*ite);
    }

    return res;
}

//删除某个属性索引
void TableManager::deleteIndex(const int col)
{
    logger.writeLog("delete index of " + to_string(col) + " ...", Logger::LogType::INFO);
    unordered_map<int, BPlusTree *>::iterator ite = m_umIndexedCol.find(col);
    if (ite == m_umIndexedCol.end())
    {
        logger.writeLog("the index of " + to_string(col) + " is not exist.", Logger::LogType::INFO);
        return;
    }
    delete ite->second;
    //删除键值对
    m_umIndexedCol.erase(ite);
    logger.writeLog("delete index of " + to_string(col) + " end.", Logger::LogType::INFO);
    return;
}

//根据记录下标得到记录
Record TableManager::getRecordByIndex(int index)
{
    if(index>=m_cBuffer.m_nRecordStartPos&&index<m_cBuffer.m_nRecordStartPos+m_cBuffer.m_nTableBufferSize)
    {
        return m_cBuffer.m_pTableBuffer[index-m_cBuffer.m_nRecordStartPos];
    }

    //重新读取缓冲区
    m_gFd[1].seekg(sizeof(Record)*index,m_gFd[1].beg);
    int data_num=min(MAX_RECORD_BUFFER_SIZE,m_nRecordNum-index);
    m_gFd[1].read((char*)m_cBuffer.m_pTableBuffer,sizeof(Record)*data_num);
    m_cBuffer.m_nRecordStartPos=index;
    m_cBuffer.m_nTableBufferSize=data_num;

    return m_cBuffer.m_pTableBuffer[0];
}

