#include<iostream>
#include<fstream>
#include<ctime>
#include"Logger.h"
using namespace std;

Logger::Logger()
{
    //打开日志
    m_fFd.open(LOG_PATH,ios::out|ios::app);
    if(!m_fFd)
    {
        cout<<"error: open log file error"<<endl;
        exit(-2);
    }

    time_t now_time=time(0);
    m_fFd<<"\n*********************Start Log*********************  TIME: "<<ctime(&now_time);


}

void Logger::writeLog(string str,LogType logType)
{
    cout<<str<<endl;
    string log_pre_text;
    switch (logType)
    {
    case LogType::INFO:
        log_pre_text="[INFO] ";
        break;
    case LogType::ERROR:
        log_pre_text="[ERROR] ";
        break;
    case LogType::DEBUG:
        log_pre_text="[DEBUG] ";
        break;
    default:
        cout<<"log error."<<endl;
        break;
    }

    time_t now_time=time(0);
    m_fFd<<log_pre_text<<": "<<str<<"             "<<ctime(&now_time);

    return;
}