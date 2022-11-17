#ifndef LOG_H
#define LOG_H

#include<fstream>

using namespace std;

#define LOG_PATH "./log.log"
class Logger
{
    private:
    fstream m_fFd;
    Logger();
    
    
    public:
    //禁止拷贝
    Logger(Logger& logger)=delete;
    Logger& operator=(Logger &logger)=delete;

    ~Logger()
    {
        m_fFd.close();
    }

    //log类型
    enum LogType
    {
        INFO,
        ERROR,
        DEBUG
    };
    //获取log单例
    static Logger& getInstance()
    {
        static Logger log;
        return log;
    }

    //写日志
    void writeLog(string str,LogType logType);

    
};

#endif