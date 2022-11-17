#include<exception>
#include<iostream>
#include<mutex>
#include<shared_mutex>
#include<string.h>
#include"BPlusTree.h"

using namespace std;

/*-----------------------BaseNode函数定义-----------------------*/
void BaseNode::setKey(int i, int64_t key)
{
    m_pKeys[i] = key;
}
//得到兄弟结点
BaseNode* BaseNode::getBrother(Direction &d)
{
    BaseNode *parent=this->getParent();
    if(parent==NULL)
    {
        return NULL;
    }

    for(int i=0;i<=parent->getKeyNum();i++)
    {
        //最右边结点返回左结点,否则返回右结点
        if(i==parent->getKeyNum())
        {
            d=LEFT;
            return ((InternalNode*)parent)->getChild(i-1);
        }
        if(((InternalNode*)parent)->getChild(i)==this)
        {
            d=RIGHT;
            return ((InternalNode*)parent)->getChild(i+1);
        }
    }
    return  NULL;
}
/*----------------------InternalNode函数定义-----------------------*/
//结点合并
bool InternalNode::mergeNode(BaseNode* node)
{
    //预留一个新的key位
    if(this->getKeyNum()+node->getKeyNum()+1>this->MAX_KEY_NUM_)
    {
        return false;
    }

    //取待合并结点的第一个孩子首值作为新的key值
    int count=this->getKeyNum();

    int64_t new_key=((InternalNode*)node)->getFirstLeafKey();
    this->setKey(count,new_key);
    this->setChild(count+1,((InternalNode*)node)->getChild(0));
    ((InternalNode*)node)->getChild(0)->setParent(this);
    count++;

    //合并其他数据
    for(int i=0;i<node->getKeyNum();i++,count++)
    {
        this->setKey(count,node->getKey(i));
        this->setChild(count+1,((InternalNode*)node)->getChild(i+1));
        ((InternalNode*)node)->getChild(i+1)->setParent(this);
    }
    this->setKeyNum(count);

    return true;
}
//得到当前结点第一个叶子结点的首值
int64_t InternalNode::getFirstLeafKey()
{
    if(this->getChild(0)->getNodeType()==LEAF_NODE)
    {
        return this->getChild(0)->getKey(0);
    }
    return ((InternalNode*)this->getChild(0))->getFirstLeafKey();
}


bool InternalNode::insert(int64_t key,BaseNode*child)
{
    //结点已满
    if(this->getKeyNum()==this->MAX_KEY_NUM_)
    {
        return false;
    }

    int i;
    for(i=this->getKeyNum();i>=1&&this->m_pKeys[i-1]>key;i--)
    {
        this->m_pKeys[i]=this->m_pKeys[i-1];
        this->m_pChilds[i+1]=m_pChilds[i];
    }

    this->m_pKeys[i]=key;
    this->m_pChilds[i+1]=child;
    this->m_nKeyNum++;
    child->setParent(this);

    return true;
    
}
//得到孩子结点下标
int InternalNode::getChildIndex(BaseNode* child)
{
    for(int i=0;i<=this->getKeyNum();i++)
    {
        if(this->getChild(i)==child)
        {
            return i;
        }
    }

    return -1;
}
//删除key值及其后面的指针
bool InternalNode::deleteKey(int64_t key)
{
    //找到删除位置
    int i;
    for(i=0;i<this->getKeyNum();i++)
    {
        if(this->getKey(i)==key)
        {
            break;
        }
    }
    if(i==this->getKeyNum())
    {
        return false;
    }

    int index=i+1;
    //数据前移
    for(i;i<this->getKeyNum()-1;i++)
    {
        this->setKey(i,this->getKey(i+1));
    }
    for(index;index<this->getKeyNum();index++)
    {
        this->setChild(index,this->getChild(index+1));
    }

    this->m_nKeyNum--;

    return true;
}

//将一个key值移动到本结点
bool InternalNode::moveOneKey(BaseNode* p_node)
{
    if(this->getKeyNum()>=this->MAX_KEY_NUM_)
    {
        return false;
    }

    //兄弟结点位于左边
    if(p_node->getKey(0)<this->getKey(0))
    {
        
        //找到最小的key值
        int64_t new_key=this->getFirstLeafKey();

        //留出空位
        for(int i=this->getKeyNum();i>=1;i--)
        {
            this->setKey(i,this->getKey(i-1));
        }
        for(int i=this->getKeyNum()+1;i>=1;i--)
        {
            this->setChild(i,this->getChild(i-1));
        }

        //修改第一个key为第一个孩子结点的首值
        this->setKey(0,new_key);
        //修改第一个子结点
        this->setChild(0,((InternalNode*)p_node)->getChild(p_node->getKeyNum()));
        this->getChild(0)->setParent(this);    
    }
    //兄弟结点位于右边
    else
    {
        //修改新加入的key值为右边结点第一个孩子的首值
        int64_t new_key=((InternalNode*)p_node)->getFirstLeafKey();
        this->setKey(this->getKeyNum(),new_key);
        BaseNode *p=((InternalNode*)p_node)->getChild(0);
        this->setChild(this->getKeyNum()+1,p);
        p->setParent(this);

        //修改兄弟结点
        for(int i=0;i<p_node->getKeyNum()-1;i++)
        {
            p_node->setKey(i,p_node->getKey(i+1));
        }
        for(int i=0;i<p_node->getKeyNum();i++)
        {
            ((InternalNode*)p_node)->setChild(i,((InternalNode*)p_node)->getChild(i+1));
        }
    }
    this->m_nKeyNum++;
    p_node->setKeyNum(p_node->getKeyNum()-1);

    return true;
}

int64_t InternalNode::split(InternalNode *brother,int64_t key)
{
    //key值位于v~v+1之间
    if(key>this->m_pKeys[this->MIN_KEY_NUM_-1]&&key<this->m_pKeys[this->MIN_KEY_NUM_])
    {
        //移动key
        for(int i=0;i<this->MIN_KEY_NUM_;i++)
        {
            //设置key
            brother->m_pKeys[i]=this->m_pKeys[i+this->MIN_KEY_NUM_];
        }
        //移动child
        for(int i=0;i<this->MIN_KEY_NUM_;i++)
        {
            //设置child
            this->m_pChilds[i+this->MIN_CHILD_NUM_]->setParent(brother);
            brother->m_pChilds[i+1]=m_pChilds[i+this->MIN_CHILD_NUM_];
        }

        this->setKeyNum(this->MIN_KEY_NUM_);
        brother->setKeyNum(this->MIN_KEY_NUM_);

        return key;
    }

    //key值位于v左边或者v+1右边
    int pos=-1;
    if(key<this->m_pKeys[this->MIN_KEY_NUM_-1])
    {
        pos=this->MIN_KEY_NUM_-1;
    }
    else
    {
        pos=this->MIN_KEY_NUM_;
    }

    //提出分界点作为返回值 这里返回值可
    int64_t retkey=this->m_pKeys[pos];
    //移动数据
    int index=0;
    for(int i=pos+1;i<this->MAX_KEY_NUM_;i++,index++)
    {
        //设置key
        brother->m_pKeys[index]=this->m_pKeys[i];
    }
    brother->setKeyNum(index);
    index=0;
    for(int i=pos+1;i<this->MAX_CHILD_NUM_;i++)
    {
        //设置child
        this->m_pChilds[i]->setParent(brother);
        brother->setChild(index++,this->m_pChilds[i]);
    }
    this->setKeyNum(pos);

    return retkey;
}
/*-----------------------------LeafNode函数定义-----------------------------------*/

void LeafNode::insert(int64_t key, int value)
{
    int i;
    for(i = this->m_nKeyNum; i >= 1 && this->m_pKeys[i-1] > key; i--)
    {
        this->m_pKeys[i] = this->m_pKeys[i - 1];
        pvalues_[i] = pvalues_[i - 1];
    }

    this->m_pKeys[i] = key;
    pvalues_[i] = value;

    this->m_nKeyNum++;
}


int64_t LeafNode::split(LeafNode *brother)
{
    brother->setKeyNum(this->MIN_KEY_NUM_);
    brother->setRightNode(getRightNode());
    brother->setLeftNode(this);
    brother->setParent(this->getParent());

    //当前结点作为左结点
    this->m_nKeyNum = this->MIN_KEY_NUM_;
    setRightNode(brother);

    //拷贝右结点的值
    for (int i = 0; i < this->MIN_KEY_NUM_; i++)
    {
        brother->setKey(i, this->m_pKeys[i + this->MIN_KEY_NUM_]);
        brother->setValue(i, pvalues_[i + this->MIN_KEY_NUM_]);
    }

    return brother->getKey(0);
}

//删除值
bool LeafNode::deleteKey(const int64_t key)
{
    int index=-1;
    for(int i=0;i<this->getKeyNum();i++)
    {
        if(this->getKey(i)==key)
        {
            index=i;
        }
    }
    if(index==-1)
    {
        return false;
    }
    //数据前移
    for(index;index<this->getKeyNum()-1;index++)
    {
        this->setKey(index,this->getKey(index+1));
        this->setValue(index,this->getValue(index+1));
    }

    this->m_nKeyNum--;

    return true;
}
//将node结点融合到本结点
bool LeafNode::mergeNode(BaseNode* node)
{
    if(this->getKeyNum()+node->getKeyNum()>this->MAX_KEY_NUM_)
    {
        return false;
    }
    for(int i=0;i<node->getKeyNum();i++)
    {
        this->insert(node->getKey(i),((LeafNode*)node)->getValue(i));
    }

    return false;
}