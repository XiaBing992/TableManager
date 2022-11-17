#ifndef BUFFER_H
#define BUFFER_H

#include"Record.h"
#include"Logger.h"

#define MAX_RECORD_BUFFER_SIZE 10000
//缓冲区
class Buffer
{
    public:
    //表数据缓冲
    Record* m_pTableBuffer;
    //对应表中的记录下标
    int m_nRecordStartPos;
    int m_nTableBufferSize;
    //用于数据buffer的互斥访问
    shared_mutex m_smTableMutex;

    //插入数据缓冲区
    Record* m_pInsertBuffer;
    int m_nInsertBufferSize;
    //用于插入数据buffer的互斥访问
    shared_mutex m_smInsertTableMutex;

    Logger& logger=Logger::getInstance();

    Buffer()
    {
        m_pTableBuffer=new Record[MAX_RECORD_BUFFER_SIZE];
        m_pInsertBuffer=new Record[MAX_RECORD_BUFFER_SIZE];
        if (!(m_pTableBuffer&&m_pInsertBuffer))
        {
            logger.writeLog("Memory allocation failure.",Logger::LogType::ERROR);
            //cout << "error: 内存分配失败." << strerror(errno) << endl;
            exit(-1);
        }
        // m_nStartPos=0;
        // m_nEndPos=0;
        m_nInsertBufferSize=0;
        m_nTableBufferSize=0;
        m_nRecordStartPos=-1;
    }
    ~Buffer()
    {
        delete m_pInsertBuffer;
        delete m_pTableBuffer;
    }

};

#endif