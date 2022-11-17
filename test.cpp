#include<iostream>
#include<time.h>
#include"Test.h"

using namespace std;

void Test::bPlusTreeInsertTest(BPlusTree* bPlusTree,int start,int end)
{
	logger.writeLog("insert num:"+to_string(end - start),Logger::LogType::INFO);
	for(int i=start;i<end;i++)
	{
		bPlusTree->insert(i,i);
	}
}

void Test::bPlusTreeInsertTestByRand(BPlusTree* bPlusTree,int n)
{
    srand((unsigned long long)time(NULL));
	logger.writeLog("insert num:"+to_string(n),Logger::LogType::INFO);
	for(int i=0;i<n;i++)
	{
		int key=rand();
		int value=rand();
		
		bPlusTree->insert(key,value);
	}
}

void Test::bPlusTreeDeleteTest(BPlusTree* bPlusTree,int start,int end)
{
	logger.writeLog("delete num:"+to_string(end -start),Logger::LogType::INFO);
	for(int i=start;i<end;i++)
	{
		bPlusTree->deleteData(i);
	}
}
void Test::bPlusTreeDeleteTestByRand(BPlusTree* bPlusTree,int n)
{
	logger.writeLog("delete num:"+to_string(n),Logger::LogType::INFO);
	srand((unsigned long long)time(NULL));
	for(int i=0;i<n;i++)
	{
		int key=rand();
		bPlusTree->deleteData(key);
	}
}

void Test::bPlusTreeSearchTest(BPlusTree* bPlusTree,int start,int end)
{
	logger.writeLog("search num:"+to_string(end - start),Logger::LogType::INFO);
	for(int i=start;i<end;i++)
	{
		bPlusTree->search(i);
	}
}
void Test::bPlusTreeSearchTestByRand(BPlusTree* bPlusTree,int n)
{
	logger.writeLog("search num:"+to_string(n),Logger::LogType::INFO);
    srand((unsigned long long)time(NULL));
	for(int i=0;i<n;i++)
	{
		int key=rand();
		bPlusTree->search(key);
	}
}

void Test::bPlusTreeUpdateTest(BPlusTree* bPlusTree,int start,int end)
{
	logger.writeLog("update num:"+to_string(end - start),Logger::LogType::INFO);
	for(int i=start;i<end;i++)
	{
		bPlusTree->update(i,i+1);
	}
}
void Test::bPlusTreeUpdateTestByRand(BPlusTree* bPlusTree,int n)
{
	logger.writeLog("update num:"+to_string(n),Logger::LogType::INFO);
    srand((unsigned long long)time(NULL));
	for(int i=0;i<n;i++)
	{
		int key=rand();
		int value=rand();
		bPlusTree->update(key,value);
	}
}

void Test::testBPlusTree()
{
    logger.writeLog("testBPlusTree...",Logger::LogType::INFO);
    BPlusTree *bPlusTree=new BPlusTree(BPLUSTREE_ORDER);
	vector<thread> thread_array;

    //测试并发
	thread_array.push_back(thread(&Test::bPlusTreeInsertTest,this,bPlusTree,0,500000));
    thread_array.push_back(thread(&Test::bPlusTreeInsertTestByRand,this,bPlusTree,300000));
    thread_array.push_back(thread(&Test::bPlusTreeDeleteTest,this,bPlusTree,200000,300000));
    thread_array.push_back(thread(&Test::bPlusTreeDeleteTestByRand,this,bPlusTree,500000));
    thread_array.push_back(thread(&Test::bPlusTreeSearchTest,this,bPlusTree,0,100000));
    thread_array.push_back(thread(&Test::bPlusTreeSearchTestByRand,this,bPlusTree,300000));
    thread_array.push_back(thread(&Test::bPlusTreeUpdateTest,this,bPlusTree,300000,400000));
    thread_array.push_back(thread(&Test::bPlusTreeUpdateTestByRand,this,bPlusTree,300000));


	for(auto ite=thread_array.begin();ite!=thread_array.end();ite++)
    {
		
		if(ite->joinable())
		{
			ite->join();
		}
        
    }

	if(bPlusTree->checkTree())
	{ 
		cout<<"满足B+树定义."<<endl;
	}
	else
	{
		cout<<"不满足B+树定义."<<endl;
	}
	delete bPlusTree;
    logger.writeLog("testBPlusTree end.",Logger::LogType::INFO);
}

//整体测试


void Test::testAll()
{
    TableManager& tableManager = TableManager::getTableManagerInstance();
	int N=2;
	vector<thread> thread_array;

	//直接搜索
	map<int,Record> res=tableManager.search(0,100,2000);
	for(auto ite=res.begin();ite!=res.end();ite++)
	{
		cout<<ite->first<<": ";
		Record r=ite->second;
		//打印前8个属性
		for(int i=0;i<8;i++)
		{
			cout<<r.m_gAttributes[i]<<" ";
		}
		cout<<endl;
	}
	
	//并发测试基本功能
	for(int i=0;i<N;i++)
	{
		Record r;
		thread_array.push_back(thread(&TableManager::createIndex,ref(tableManager),i));
		thread_array.push_back(thread(&TableManager::append,ref(tableManager),ref(r)));
	}
	for(auto ite=thread_array.begin();ite!=thread_array.end();ite++)
	{
		ite->join();
	}

	//使用索引查找数据
	res=tableManager.search(0,100,2000);
	for(auto ite=res.begin();ite!=res.end();ite++)
	{
		cout<<ite->first<<": ";
		Record r=ite->second;
		//打印前8个属性
		for(int i=0;i<8;i++)
		{
			cout<<r.m_gAttributes[i]<<" ";
		}
		cout<<endl;
	}

}