#include<iostream>
#include<string.h>
#include"TableManager.h"
#include "Record.h"

Record::Record()
{
    memset(m_gAttributes,0,sizeof(m_gAttributes));
    if(!m_gAttributes)
    {
        cout<<"error: "<<strerror(errno)<<endl;
        exit(-1);
    }
}