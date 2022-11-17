#ifndef BPLUSTREENode_H
#define BPLUSTREENode_H
#include<exception>
#include<iostream>
#include<mutex>
#include<shared_mutex>
#include<string.h>

using namespace std;

#define MAX_ALLOW_CHILD_NUM 256
//结点类型
enum NodeType{
    ROOT_NODE,
    INTERNAL_NODE,
    LEAF_NODE,
};
//兄弟方向
enum Direction{
    LEFT,
    RIGHT,
};
//结点基类
class BaseNode
{
    protected:
    //BaseNode *m_pChilds[MAX_ALLOW_CHILD_NUM];
    //结点类型
    NodeType m_eNodeType;
    //关键字个数
    int m_nKeyNum;
    //存放key值
    int64_t *m_pKeys;
    //记录阶数
    int m_nN;
    //记录允许最小关键字数、最小孩子数
    int MIN_KEY_NUM_;
    int MIN_CHILD_NUM_;
    int MAX_KEY_NUM_;
    int MAX_CHILD_NUM_;

    //指向父结点
    BaseNode *m_pParentNode;
    //锁
    shared_mutex m_smMutex;
    
    public:
    //传入树的阶数
    BaseNode(int n)
    {
        //分配最大含有关键字的大小
        m_pKeys=new int64_t[n-1];
        m_nKeyNum=0;
        m_eNodeType=LEAF_NODE;
        m_nN=n;
        MIN_KEY_NUM_=n/2;
        MIN_CHILD_NUM_=n/2+1;
        MAX_KEY_NUM_=n-1;
        MAX_CHILD_NUM_=n;

        m_pParentNode=NULL;
    }
    virtual ~BaseNode()
    {
        delete []m_pKeys;
    }
    //get函数
    int getN() const
    {
        return m_nN;
    }
    int getKeyNum() const
    {
        return m_nKeyNum;
    }
    int getNodeType() const
    {
        return m_eNodeType;
    }
    int64_t getKey(int i)
    {
        return m_pKeys[i];
    }
    int getKeyIndex(int64_t key);
    int getMinKeyNum()
    {
        return MIN_KEY_NUM_;
    }
    int getMinChildNum()
    {
        return MIN_CHILD_NUM_;
    }
    BaseNode *getParent()
    {
        return m_pParentNode;
    }
    //得到兄弟结点
    BaseNode* getBrother(Direction &d);
    shared_mutex& getSharedMutex()
    {
        return m_smMutex;
    }
    //set函数
    void setNodeType(NodeType t)
    {
        m_eNodeType=t;
    }
    void setKeyNum(int n)
    {
        m_nKeyNum=n;
    }
    void setKey(int i,int64_t key);
    void setMinKeyNum(int n)
    {
        MIN_KEY_NUM_=n;
    }
    void setMinChildNum(int n)
    {
        MIN_CHILD_NUM_=n;
    }
    void setParent(BaseNode* p)
    {
        m_pParentNode=p;
    }
    //结点合并
    virtual bool mergeNode(BaseNode* node)=0;
};

//内部结点
class InternalNode:public BaseNode
{
    private:
    //孩子结点
    BaseNode *m_pChilds[MAX_ALLOW_CHILD_NUM];

    public:
    InternalNode(int n):BaseNode(n)
    {
        this->m_eNodeType=INTERNAL_NODE;
        memset(this->m_pChilds,0,sizeof(m_pChilds));
    }
    virtual ~InternalNode()
    {
        //delete []pchilds_;
    }
    
    BaseNode* getChild(int i)
    {
        return m_pChilds[i];
    }
    void setChild(int i,BaseNode *child)
    {
        m_pChilds[i]=child;
    }
    //将一个key值移动到本结点
    bool moveOneKey(BaseNode* p_node);
    //得到当前结点第一个叶子结点的首值
    int64_t getFirstLeafKey();
    //结点合并
    virtual bool mergeNode(BaseNode* node);
    //得到孩子结点下标
    int getChildIndex(BaseNode* child);
    bool insert(int64_t key,BaseNode* child);
    //brother为分裂结点，key为新插入的key
    int64_t split(InternalNode *brother,int64_t key);
    //删除key值及其后面的指针
    bool deleteKey(int64_t key);
};

//实现叶子结点
class LeafNode:public BaseNode
{
    private:
    //左右结点
    LeafNode *pleft_node_;
    LeafNode *pright_node_;
    //存放值
    int *pvalues_;

    public:
    LeafNode(int n):BaseNode(n)
    {
        pvalues_=new int[n-1];
        this->m_eNodeType=LEAF_NODE;
        //this->key_num_=0;
        pleft_node_=NULL;
        pright_node_=NULL;
    }
    virtual ~LeafNode()
    {
        delete []pvalues_;
    }
    //get函数
    LeafNode* getLeftNode()
    {
        return pleft_node_;
    }
    LeafNode* getRightNode()
    {
        return pright_node_;
    }
    int getValue(int i)
    {
        return pvalues_[i];
    }

    //set函数
    void setLeftNode(LeafNode *p)
    {
        pleft_node_=p;
    }
    void setRightNode(LeafNode *p)
    {
        pright_node_=p;
    }
    void setValue(int i,int value)
    {
        pvalues_[i]=value;
    }

    void insert(int64_t key,int value);

    //结点合并
    virtual bool mergeNode(BaseNode* node);
    //叶子结点分裂
    int64_t split(LeafNode *brother);
    //删除值
    bool deleteKey(const int64_t key);
};

#endif