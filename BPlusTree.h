#ifndef BPLUSTREE_H
#define BPLUSTREE_H
#include <iostream>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <queue>
#include <stack>
#include<future>
#include <sstream>
#include <string>
#include "Logger.h"
#include "BPlusTreeNode.h"

using namespace std;
#define SEARCH_ERROR INT32_MIN

class BPlusTree
{
private:
    BaseNode *m_pRoot;
    //第一个叶子结点
    LeafNode *m_pDataHead;
    //最大key
    int64_t m_nMaxKey;

    //树的基本信息
    int m_nN;
    int MIN_KEY_NUM_;
    int MIN_CHILD_NUM_;
    int MAX_KEY_NUM_;
    int MAX_CHILD_NUM;
    //保存的数据数
    int m_nDataNum;
    //用于根节点为空时的互斥访问
    shared_mutex m_smProotMutex;

    //日志
    Logger& logger=Logger::getInstance();

    //将栈中结点全部解锁
    void unLockStack(stack<unique_lock<shared_mutex>> *st_lock_node_, unique_lock<shared_mutex> &proot_guard);

    //查找小于key值的第一个叶子结点
    LeafNode *searchInsertLeafNode(int64_t key, unique_lock<shared_mutex> &proot_guard, stack<unique_lock<shared_mutex>> *st_lock_node_);
    LeafNode *searchDeleteLeafNode(int64_t key, unique_lock<shared_mutex> &proot_guard, stack<unique_lock<shared_mutex>> *st_lock_node_);

    //中间结点插入key值
    bool insertInternalNode(BaseNode *parent, int64_t key, BaseNode *right_node, stack<unique_lock<shared_mutex>> *st_lock_node_, unique_lock<shared_mutex> &proot_guard);
    //中间结点删除key值
    bool deleteInternalNode(BaseNode *p_node, int delete_index, stack<unique_lock<shared_mutex>> *st_lock_node_, unique_lock<shared_mutex> &proot_guard);
    //检查内部结点是否满足定义
    bool checkInternalNode(BaseNode *now_node);
    //检查一个结点中的属性是否满足B+树
    bool checkOneNode(BaseNode *p_node);

public:
    BPlusTree(int n)
    {
        m_pRoot = NULL;
        m_pDataHead = NULL;

        m_nN = n;
        MIN_KEY_NUM_ = n / 2;
        MIN_CHILD_NUM_ = n / 2 + 1;
        MAX_KEY_NUM_ = n - 1;
        MAX_CHILD_NUM = n;

        m_nDataNum = 0;
    };
    ~BPlusTree()
    {
        //BFS释放内存
        queue<BaseNode*> qu;
        if(m_pRoot!=NULL)
        {
            qu.push(m_pRoot);
        }
        while(!qu.empty())
        {
            BaseNode* p=qu.front();
            for(int i=0;i<p->getKeyNum()&&p->getNodeType()!=LEAF_NODE;i++)
            {
                qu.push(((InternalNode*)p)->getChild(i));
                
            }
            qu.pop();
            delete p;
        }
        // if (m_pRoot != m_pDataHead)
        // {
        //     delete m_pRoot;
        //     delete m_pDataHead;
        // }
        // else
        // {
        //     delete m_pRoot;
        // }
    }

    BaseNode *getProot()
    {
        return m_pRoot;
    }
    int getDataNum()
    {
        return m_nDataNum;
    }
    int getMinKeyNum()
    {
        return MIN_KEY_NUM_;
    }
    int getMaxKeyNum()
    {
        return MAX_KEY_NUM_;
    }
    int getMinChildNum()
    {
        return MIN_CHILD_NUM_;
    }
    int getMaxChildNum()
    {
        return MAX_CHILD_NUM;
    }
    bool setProot(BaseNode *p_node)
    {
        m_pRoot = p_node;
    }
    bool setPDataHead(LeafNode *p_node)
    {
        m_pDataHead=p_node;
    }

    //键值插入
    bool insert(const int64_t key, const int value);
    //键值删除
    bool deleteData(int64_t key);
    //修改
    bool update(int64_t key, int value);
    //查找数据
    int search(int64_t key);
    //查找范围内的数据
    vector<int> searchRange(const int low,const int high,promise<vector<int>> *promise_ret);
    // valueType searchData(keyType key);

    //检查是否满足B+树的定义
    bool checkTree();
    // B+树的序列化与反序列化
    bool serialization(string file_name);
    static BPlusTree *deSerialization(string file_name);
};


#endif