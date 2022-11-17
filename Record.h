#ifndef RECORD_H
#define RECORD_H

#include<unordered_map>
#include<shared_mutex>
#include<string>
#include<vector>

using namespace std;

#define ATTRIBUTE_NUM 100


//表的每一条记录
class Record
{
    
    public:
    //属性
    int64_t m_gAttributes[ATTRIBUTE_NUM];

    Record();
    Record(Record& record)
    {
        copy(begin(record.m_gAttributes),end(record.m_gAttributes),begin(this->m_gAttributes));
    }
    Record operator=(const Record& record)
    {
        copy(begin(record.m_gAttributes),end(record.m_gAttributes),begin(this->m_gAttributes));

        return *this;
    }
    Record(Record&& record)
    {
        copy(begin(record.m_gAttributes),end(record.m_gAttributes),begin(this->m_gAttributes));
    }
    
};

#endif