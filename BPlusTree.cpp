#include <iostream>
#include <iomanip>
#include <mutex>
#include <shared_mutex>
#include <queue>
#include <stack>
#include <fstream>
#include <string>
#include "BPlusTree.h"

using namespace std;
//更新
bool BPlusTree::update(int64_t key, int value)
{
    unique_lock<shared_mutex> proot_guard(m_smProotMutex);
    if (m_pRoot == NULL)
    {
        return false;
    }

    //从根结点向下搜索
    BaseNode *p = m_pRoot;
    queue<unique_lock<shared_mutex>> qu_locked_node;
    qu_locked_node.push(unique_lock<shared_mutex>(p->getSharedMutex()));
    int h = 0;
    while (p != NULL)
    {
        h++;
        //找到叶子结点结束
        if (p->getNodeType() == LEAF_NODE)
        {
            break;
        }

        int i;
        for (i = 0; i < p->getKeyNum(); i++)
        {
            if (key < p->getKey(i))
            {
                break;
            }
        }
        qu_locked_node.push(unique_lock<shared_mutex>((((InternalNode *)p)->getChild(i)->getSharedMutex())));
        if (p != m_pRoot)
        {
            //拿到当前层结点锁弹出上上层结点
            qu_locked_node.pop();
            if (proot_guard.owns_lock())
            {
                proot_guard.unlock();
            }
        }
        p = ((InternalNode *)p)->getChild(i);
    }

    for (int i = 0; i < p->getKeyNum(); i++)
    {
        if (((LeafNode *)p)->getKey(i) == key)
        {
            ((LeafNode *)p)->setValue(i, value);
            return true;
        }
    }
    return false;
}

bool BPlusTree::serialization(string file_name)
{
    logger.writeLog("BPlusTree serialization...", Logger::LogType::INFO);
    fstream data(file_name, ios::in | ios::out | ios::trunc);
    if (!data)
    {
        logger.writeLog("open file error.", Logger::LogType::ERROR);
        return false;
    }

    if (!m_pRoot)
    {
        data << "NULL" << endl;
        return true;
    }
    data << m_pRoot->getN() << endl;

    //存储结点
    queue<BaseNode *> qe_node;
    qe_node.push(m_pRoot);

    // BFS
    while (!qe_node.empty())
    {
        BaseNode *now_node = qe_node.front();
        qe_node.pop();

        //叶子结点时,记录value
        if (now_node->getNodeType() == LEAF_NODE)
        {
            data << "leaf_key_num: " << now_node->getKeyNum() << " key: ";
            for (int i = 0; i < now_node->getKeyNum(); i++)
            {
                data << now_node->getKey(i) << " ";
            }
            data << "value: ";
            for (int i = 0; i < now_node->getKeyNum(); i++)
            {
                data << ((LeafNode *)now_node)->getValue(i) << " ";
            }
            data << endl;
            // continue;
        }
        else
        {
            data << "key_num: " << now_node->getKeyNum() << " key: ";
            for (int i = 0; i < now_node->getKeyNum(); i++)
            {
                data << now_node->getKey(i) << " ";
            }
            data << endl;
            for (int i = 0; i <= now_node->getKeyNum(); i++)
            {
                BaseNode *now_child = ((InternalNode *)now_node)->getChild(i);
                qe_node.push(now_child);
            }
        }
    }
    data.close();

    logger.writeLog("serialization end.", Logger::LogType::INFO);
    return true;
}
BPlusTree *BPlusTree::deSerialization(string file_name)
{
    fstream data(file_name);
    if (!data)
    {
        Logger::getInstance().writeLog("open error.", Logger::LogType::ERROR);
        return NULL;
    }
    Logger::getInstance().writeLog("BPlusTree deSerialization...", Logger::LogType::INFO);
    int n;
    string str_info;
    int key_count;
    int64_t key_data;
    queue<BaseNode *> qe_node;

    //数据处理
    data >> n;
    BPlusTree *b_plus_tree = new BPlusTree(n);
    data >> str_info;
    //叶子结点
    if (str_info == "leaf_key_num:")
    {
        LeafNode *new_node = new LeafNode(n);
        data >> key_count >> str_info;
        new_node->setKeyNum(key_count);
        for (int i = 0; i < key_count; i++)
        {
            data >> key_data;
            new_node->setKey(i, key_data);
        }
        data >> str_info;
        for (int i = 0; i < key_count; i++)
        {
            data >> key_data;
            new_node->setValue(i, key_data);
        }

        b_plus_tree->setProot(new_node);
        b_plus_tree->setPDataHead(new_node);
        new_node->setLeftNode(NULL);
        new_node->setRightNode(NULL);

        return b_plus_tree;
    }
    else
    {
        InternalNode *new_node = new InternalNode(n);
        data >> key_count >> str_info;
        new_node->setKeyNum(key_count);
        for (int i = 0; i < key_count; i++)
        {
            data >> key_data;
            new_node->setKey(i, key_data);
        }
        b_plus_tree->setProot(new_node);
        qe_node.push(new_node);
    }
    int child_count;
    BaseNode *old_node = NULL;
    while (!qe_node.empty())
    {
        BaseNode *now_node = qe_node.front();
        qe_node.pop();
        child_count = now_node->getKeyNum() + 1;

        //创建孩子结点
        for (int c = 0; c < child_count; c++)
        {
            data >> str_info;
            data >> key_count;
            // cout<<str_info<<endl;
            if (str_info == "leaf_key_num:")
            {
                LeafNode *new_node = new LeafNode(n);
                new_node->setKeyNum(key_count);
                ((InternalNode *)now_node)->setChild(c, new_node);
                data >> str_info;
                for (int i = 0; i < key_count; i++)
                {
                    data >> key_data;
                    new_node->setKey(i, key_data);
                }
                data >> str_info;
                for (int i = 0; i < key_count; i++)
                {
                    data >> key_data;
                    new_node->setValue(i, key_data);
                }
                //还原双向链表
                if (!old_node)
                {
                    b_plus_tree->m_pDataHead = new_node;
                    new_node->setLeftNode(NULL);
                    new_node->setRightNode(NULL);
                    old_node = new_node;
                }
                else
                {
                    ((LeafNode *)old_node)->setRightNode(new_node);
                    new_node->setLeftNode((LeafNode *)old_node);
                    new_node->setRightNode(NULL);
                    old_node = new_node;
                }
            }
            else
            {
                InternalNode *new_node = new InternalNode(n);
                new_node->setKeyNum(key_count);
                ((InternalNode *)now_node)->setChild(c, new_node);
                data >> str_info;
                for (int i = 0; i < key_count; i++)
                {
                    data >> key_data;
                    new_node->setKey(i, key_data);
                }
                new_node->setParent(now_node);
                qe_node.push(new_node);
            }
        }
    }
    data.close();
    Logger::getInstance().writeLog("BPlusTree deSerialization end.", Logger::LogType::INFO);

    return b_plus_tree;
}
//键值插入
bool BPlusTree::insert(const int64_t key, const int value)
{
    unique_lock<shared_mutex> proot_guard(m_smProotMutex);
    if (m_pRoot == NULL)
    {
        LeafNode *new_node = new LeafNode(m_nN);
        new_node->setKey(0, key);
        new_node->setValue(0, value);
        new_node->setKeyNum(1);
        new_node->setParent(NULL);

        m_pRoot = new_node;
        m_pDataHead = new_node;
        new_node->setLeftNode(NULL);
        new_node->setRightNode(NULL);

        m_nDataNum++;

        return true;
    }
    //存储上锁了的结点
    stack<unique_lock<shared_mutex>> *st_locked_node = new stack<unique_lock<shared_mutex>>();
    //查看key是否已经存在
    LeafNode *first_leaf_node = searchInsertLeafNode(key, proot_guard, st_locked_node);

    for (int i = 0; i < first_leaf_node->getKeyNum(); i++)
    {
        if (first_leaf_node->getKey(i) == key)
        {
            unLockStack(st_locked_node, proot_guard);
            return false;
        }
    }
    m_nDataNum++;

    BaseNode *parent = first_leaf_node->getParent();
    //叶子结点未满直接插入
    if (first_leaf_node->getKeyNum() < this->getMaxKeyNum())
    {
        first_leaf_node->insert(key, value);
        unLockStack(st_locked_node, proot_guard);
        return true;
    }

    //创建右结点
    LeafNode *brother = new LeafNode(m_nN);
    //结点分裂
    int64_t new_key = first_leaf_node->split(brother);
    //数据插入
    if (key < new_key)
    {
        first_leaf_node->insert(key, value);
    }
    else
    {
        brother->insert(key, value);
    }

    //叶子结点已满，且根结点就是叶子结点,分裂叶节点
    if (m_pRoot->getNodeType() == LEAF_NODE)
    {
        //创建新的父结点
        InternalNode *new_root = new InternalNode(m_nN);

        new_root->setKeyNum(1);
        new_root->setKey(0, new_key);

        //当前结点作为左结点
        new_root->setChild(0, m_pRoot);
        new_root->setChild(1, brother);
        //设置父结点
        m_pRoot->setParent(new_root);
        brother->setParent(new_root);
        //更新双向链表
        ((LeafNode *)m_pRoot)->setRightNode(brother);
        brother->setRightNode(NULL);

        m_pRoot = new_root;

        unLockStack(st_locked_node, proot_guard);
        return true;
    }

    st_locked_node->pop();
    // 1.叶子结点已满,父结点未满;2.叶子结点与父结点都满了，从下到上分裂
    insertInternalNode(parent, new_key, brother, st_locked_node, proot_guard);
    unLockStack(st_locked_node, proot_guard);
    delete st_locked_node;
    return true;
}

bool BPlusTree::insertInternalNode(BaseNode *p_node, int64_t key, BaseNode *right_node, stack<unique_lock<shared_mutex>> *st_locked_node, unique_lock<shared_mutex> &proot_guard)
{
    if (p_node == NULL)
    {
        unLockStack(st_locked_node, proot_guard);
        return false;
    }

    //父结点未满，直接插入
    if (p_node->getKeyNum() < MAX_KEY_NUM_)
    {
        return ((InternalNode *)p_node)->insert(key, right_node);
    }

    //父结点已满，分配新结点
    InternalNode *parent_right_node = new InternalNode(m_nN);
    //当前父结点分裂
    int64_t new_key = ((InternalNode *)p_node)->split(parent_right_node, key);

    // key值插入
    if (p_node->getKeyNum() < parent_right_node->getKeyNum())
    {
        ((InternalNode *)p_node)->insert(key, right_node);
    }
    else if (p_node->getKeyNum() > parent_right_node->getKeyNum())
    {
        ((InternalNode *)parent_right_node)->insert(key, right_node);
    }
    //直接挂到右边
    else
    {
        parent_right_node->setChild(0, right_node);
        right_node->setParent(parent_right_node);
    }
    BaseNode *parent = p_node->getParent();
    // BaseNode<keyType,valueType> *grand_parent=parent->getParent();
    //父结点是空
    if (parent == NULL)
    {
        parent = new InternalNode(m_nN);
        ((InternalNode *)parent)->setChild(0, p_node);
        ((InternalNode *)parent)->setKeyNum(1);
        ((InternalNode *)parent)->setKey(0, new_key);
        ((InternalNode *)parent)->setChild(1, parent_right_node);

        ((InternalNode *)p_node)->setParent(parent);
        parent_right_node->setParent(parent);

        m_pRoot = parent;
        unLockStack(st_locked_node, proot_guard);
        return true;
    }
    st_locked_node->pop();
    return insertInternalNode(parent, new_key, parent_right_node, st_locked_node, proot_guard);
}
//键值删除
bool BPlusTree::deleteData(int64_t key)
{
    unique_lock<shared_mutex> proot_guard(m_smProotMutex);
    if (m_pRoot == NULL)
    {
        return false;
    }

    stack<unique_lock<shared_mutex>> *st_lock_node_ = new stack<unique_lock<shared_mutex>>();
    LeafNode *p = searchDeleteLeafNode(key, proot_guard, st_lock_node_);


    //从叶子结点删除数据
    bool temp = p->deleteKey(key);
    if (!temp)
    {
        unLockStack(st_lock_node_, proot_guard);
        return false;
    }
    m_nDataNum--;
    BaseNode *parent = p->getParent();
    if (parent == NULL)
    {
        if (p->getKeyNum() == 0)
        {
            m_pRoot = NULL;
            m_pDataHead = NULL;

            unLockStack(st_lock_node_, proot_guard);
            delete p;
            return true;
        }
        unLockStack(st_lock_node_, proot_guard);
        return true;
    }

    //删除后叶子结点依然大于MIN_KEY_NUM_
    if (p->getKeyNum() >= MIN_KEY_NUM_)
    {
        unLockStack(st_lock_node_, proot_guard);
        return true;
    }
    //兄弟结点方向
    Direction d;
    //这里只能上非阻塞锁
    unique_lock<shared_mutex> brother_lock(p->getBrother(d)->getSharedMutex(), try_to_lock);
    while (!brother_lock.owns_lock())
    {
        brother_lock = unique_lock<shared_mutex>(p->getBrother(d)->getSharedMutex(), try_to_lock);
    }
    st_lock_node_->push(move(brother_lock));
    BaseNode *brother = p->getBrother(d);

    //若兄弟结点数目大于MIN_KEY_NUM_
    int64_t move_key;
    int move_value;
    if (brother->getKeyNum() > MIN_KEY_NUM_)
    {
        if (d == LEFT)
        {
            move_key = brother->getKey(brother->getKeyNum() - 1);
            move_value = ((LeafNode *)brother)->getValue(brother->getKeyNum() - 1);
        }
        else
        {
            move_key = brother->getKey(0);
            move_value = ((LeafNode *)brother)->getValue(0);
        }
        //转移数据
        p->insert(move_key, move_value);
        ((LeafNode *)brother)->deleteKey(move_key);

        //修改父结点key值
        if (brother->getNodeType() == LEFT)
        {
            for (int i = 0; i <= parent->getKeyNum(); i++)
            {
                if (((InternalNode *)parent)->getChild(i) == p)
                {
                    parent->setKey(i - 1, p->getKey(0));
                    break;
                }
            }
        }
        //兄弟结点为右边时，当前结点和兄弟结点的父结点key值都可能不等于首元素值
        else
        {
            for (int i = 0; i <= parent->getKeyNum(); i++)
            {
                if (((InternalNode *)parent)->getChild(i) == p && i >= 1)
                {
                    parent->setKey(i - 1, p->getKey(0));
                }
                if (((InternalNode *)parent)->getChild(i) == brother)
                {
                    parent->setKey(i - 1, brother->getKey(0));
                    break;
                }
            }
        }

        unLockStack(st_lock_node_, proot_guard);
        return true;
    }
    //兄弟结点不够借,合并兄弟结点
    int64_t new_key;
    //用于记录删除结点的index值
    int delete_index;
    if (d == LEFT)
    {
        ((LeafNode *)brother)->mergeNode(p);
        //获得要父节点删除的index
        delete_index = ((InternalNode *)parent)->getChildIndex(brother);

        //修改双向链表
        LeafNode *b_right = p->getRightNode();
        ((LeafNode *)brother)->setRightNode(b_right);
        if (b_right)
        {
            b_right->setLeftNode((LeafNode *)brother);
        }

        //解锁兄弟结点
        st_lock_node_->pop();
        //解锁当前结点
        st_lock_node_->pop();
        delete p;
    }
    else
    {
        p->mergeNode(brother);
        //获得要删除的key的index
        delete_index = ((InternalNode *)parent)->getChildIndex(p);

        //修改双向链表
        LeafNode *p_right = ((LeafNode *)brother)->getRightNode();
        p->setRightNode(p_right);
        if (p_right)
        {
            p_right->setLeftNode(p);
        }

        //删除被融合结点
        // brother->setKeyNum(0);

        //解锁兄弟结点
        st_lock_node_->pop();
        //解锁当前结点
        st_lock_node_->pop();

        delete brother;
    }

    deleteInternalNode(parent, delete_index, st_lock_node_, proot_guard);
    unLockStack(st_lock_node_, proot_guard);
    delete st_lock_node_;
    return true;
}
//中间结点删除key值 注意：只有在左右结点变了方向时才需要去改父结点的key值(1.从兄弟结点借 2.删除结点)
bool BPlusTree::deleteInternalNode(BaseNode *p_node, int delete_index, stack<unique_lock<shared_mutex>> *st_locked_node, unique_lock<shared_mutex> &proot_guard)
{
    bool temp = ((InternalNode *)p_node)->deleteKey(p_node->getKey(delete_index));
    if (!temp)
    {
        return false;
    }
    BaseNode *parent = p_node->getParent();
    if (parent == NULL)
    {
        if (p_node->getKeyNum() == 0)
        {
            m_pRoot = ((InternalNode *)p_node)->getChild(0);
            m_pRoot->setParent(NULL);

            unLockStack(st_locked_node, proot_guard);
            delete p_node;
            return true;
        }
        unLockStack(st_locked_node, proot_guard);
        return true;
    }
    //删除key后数量依然大于MIN_KEY_NUM_
    if (p_node->getKeyNum() >= MIN_KEY_NUM_)
    {
        unLockStack(st_locked_node, proot_guard);
        return true;
    }
    //寻找兄弟结点
    Direction d;
    unique_lock<shared_mutex> brother_lock(p_node->getBrother(d)->getSharedMutex(), try_to_lock);
    while (!brother_lock.owns_lock())
    {
        brother_lock = unique_lock<shared_mutex>(p_node->getBrother(d)->getSharedMutex(), try_to_lock);
    }
    st_locked_node->push(move(brother_lock));

    BaseNode *brother = p_node->getBrother(d);
    //兄弟结点够借
    if (brother->getKeyNum() > MIN_KEY_NUM_)
    {
        ((InternalNode *)p_node)->moveOneKey(brother);
        if (d == LEFT)
        {
            //修改父结点的key值
            for (int i = 0; i <= parent->getKeyNum(); i++)
            {
                if (((InternalNode *)parent)->getChild(i) == p_node)
                {
                    parent->setKey(i - 1, ((InternalNode *)p_node)->getFirstLeafKey());
                    break;
                }
            }
        }
        else
        {
            //修改父结点的key值
            for (int i = 0; i <= parent->getKeyNum(); i++)
            {
                if (((InternalNode *)parent)->getChild(i) == p_node && i >= 1)
                {
                    parent->setKey(i - 1, ((InternalNode *)p_node)->getFirstLeafKey());
                }
                if (((InternalNode *)parent)->getChild(i) == brother)
                {
                    parent->setKey(i - 1, ((InternalNode *)brother)->getFirstLeafKey());
                    break;
                }
            }
        }
        unLockStack(st_locked_node, proot_guard);
        return true;
    }

    // int64_t new_key;
    if (d == LEFT)
    {
        ((InternalNode *)brother)->mergeNode(p_node);

        delete_index = ((InternalNode *)parent)->getChildIndex(brother);

        st_locked_node->pop();
        st_locked_node->pop();
        delete p_node;
    }
    else
    {
        ((InternalNode *)p_node)->mergeNode(brother);

        delete_index = ((InternalNode *)parent)->getChildIndex(p_node);

        brother->setKeyNum(0);
        st_locked_node->pop();
        st_locked_node->pop();
        delete brother;
    }
    //继续修改父结点的key值
    return deleteInternalNode(parent, delete_index, st_locked_node, proot_guard);
}

//查找小于key值的第一个叶子结点
LeafNode *BPlusTree::searchInsertLeafNode(int64_t key, unique_lock<shared_mutex> &proot_guard, stack<unique_lock<shared_mutex>> *st_locked_node)
{
    BaseNode *p = m_pRoot;
    st_locked_node->push(unique_lock<shared_mutex>(p->getSharedMutex()));

    while (p != NULL)
    {
        //找到叶子结点结束
        if (p->getNodeType() == LEAF_NODE)
        {
            return (LeafNode *)p;
        }

        int i;
        for (i = 0; i < p->getKeyNum(); i++)
        {
            if (key < p->getKey(i))
            {
                break;
            }
        }
        //当前结点处于安全状态就解锁祖先结点
        if (p->getKeyNum() < this->getMaxKeyNum() && p != m_pRoot)
        {
            //转移当前结点控制权
            unique_lock<shared_mutex> transfor_lock(move(st_locked_node->top()));
            unLockStack(st_locked_node, proot_guard);
            //转移回去
            st_locked_node->push(move(transfor_lock));
        }

        st_locked_node->push(unique_lock<shared_mutex>(((InternalNode *)p)->getChild(i)->getSharedMutex()));
        p = ((InternalNode *)p)->getChild(i);
    }
    return (LeafNode *)p;
}
LeafNode *BPlusTree::searchDeleteLeafNode(int64_t key, unique_lock<shared_mutex> &proot_guard, stack<unique_lock<shared_mutex>> *st_locked_node)
{
    BaseNode *p = m_pRoot;

    st_locked_node->push(unique_lock<shared_mutex>(p->getSharedMutex()));

    while (p != NULL)
    {
        //找到叶子结点结束
        if (p->getNodeType() == LEAF_NODE)
        {
            return (LeafNode *)p;
        }

        int i;
        for (i = 0; i < p->getKeyNum(); i++)
        {
            if (key < p->getKey(i))
            {
                break;
            }
        }
        //当前结点处于安全状态就解锁祖先结点(当前是根节点不能解锁)
        if (p->getKeyNum() > this->getMinKeyNum() && p != m_pRoot)
        {
            //转移当前结点控制权
            unique_lock<shared_mutex> transfor_lock(move(st_locked_node->top()));
            unLockStack(st_locked_node, proot_guard);
            //转移回去
            st_locked_node->push(move(transfor_lock));
        }
        st_locked_node->push(unique_lock<shared_mutex>(((InternalNode *)p)->getChild(i)->getSharedMutex()));
        p = ((InternalNode *)p)->getChild(i);
    }
    return (LeafNode *)p;
}

//查找key
int BPlusTree::search(int64_t key)
{
    shared_lock<shared_mutex> proot_guard(m_smProotMutex);
    if (m_pRoot == NULL)
    {
        return SEARCH_ERROR;
    }

    //从根结点向下搜索
    BaseNode *p = m_pRoot;
    queue<shared_lock<shared_mutex>> qu_locked_node;
    qu_locked_node.push(shared_lock<shared_mutex>(p->getSharedMutex()));

    while (p != NULL)
    {
        //找到叶子结点结束
        if (p->getNodeType() == LEAF_NODE)
        {
            break;
        }
        int i;
        for (i = 0; i < p->getKeyNum(); i++)
        {
            if (key < p->getKey(i))
            {
                break;
            }
        }

        qu_locked_node.push(shared_lock<shared_mutex>((((InternalNode *)p)->getChild(i)->getSharedMutex())));
        if (p != m_pRoot)
        {
            //拿到当前层结点锁弹出上上层结点
            qu_locked_node.pop();
            if (proot_guard.owns_lock())
            {
                proot_guard.unlock();
            }
        }
        p = ((InternalNode *)p)->getChild(i);
    }

    for (int i = 0; i < p->getKeyNum(); i++)
    {
        if (((LeafNode *)p)->getKey(i) == key)
        {
            return ((LeafNode *)p)->getValue(i);
        }
    }
    return SEARCH_ERROR;
}

//检查是否满足B+树的定义
bool BPlusTree::checkTree()
{
    //检查叶子结点
    LeafNode *p_leaf = m_pDataHead;
    if (!p_leaf)
    {
        return true;
    }

    while (p_leaf)
    {
        for (int i = 0; i < p_leaf->getKeyNum() - 1; i++)
        {
            if (p_leaf->getKey(i) > p_leaf->getKey(i + 1))
            {
                return false;
            }
        }
        p_leaf = p_leaf->getRightNode();
    }

    //检查内部结点
    return checkInternalNode(m_pRoot);
}

bool BPlusTree::checkInternalNode(BaseNode *now_node)
{
    if (!now_node)
    {
        return true;
    }
    //检查当前结点是否满足定义
    checkOneNode(now_node);

    if (now_node->getNodeType() == LEAF_NODE)
    {
        return true;
    }
    //递归检查孩子结点
    for (int i = 0; i <= now_node->getKeyNum(); i++)
    {
        if (!checkInternalNode(((InternalNode *)now_node)->getChild(i)))
        {
            return false;
        }
    }

    return true;
}


//检查一个结点中的属性是否满足B+树
bool BPlusTree::checkOneNode(BaseNode *p_node)
{
    //检查数目
    int key_num = p_node->getKeyNum();
    int child_num = key_num + 1;
    if (key_num < this->getMinKeyNum() || key_num > this->getMaxKeyNum())
    {
        return false;
    }

    //检查key
    for (int i = 0; i < key_num - 1; i++)
    {
        if (p_node->getKey(i) >= p_node->getKey(i + 1))
        {
            return false;
        }
    }

    return true;
}

void BPlusTree::unLockStack(stack<unique_lock<shared_mutex>> *st_locked_node, unique_lock<shared_mutex> &proot_guard)
{
    while (!st_locked_node->empty())
    {
        st_locked_node->pop();
    }
    if (proot_guard.owns_lock())
    {
        unique_lock<shared_mutex> u(move(proot_guard));
    }
}
//查找范围内的数据
vector<int> BPlusTree::searchRange(const int low, const int high, promise<vector<int>> *promise_ret)
{
    vector<int> res;
    shared_lock<shared_mutex> proot_guard(m_smProotMutex);
    if (m_pRoot == NULL)
    {
        return res;
    }

    /*-----------找到满足的第一个叶子结点-------------*/
    //从根结点向下搜索
    int first_key = low;
    BaseNode *p;
    while (first_key <= high)
    {
        p = m_pRoot;
        queue<shared_lock<shared_mutex>> qu_locked_node;
        qu_locked_node.push(shared_lock<shared_mutex>(p->getSharedMutex()));
        while (p != NULL)
        {
            //找到叶子结点结束
            if (p->getNodeType() == LEAF_NODE)
            {
                break;
            }
            int i;
            for (i = 0; i < p->getKeyNum(); i++)
            {
                if (first_key < p->getKey(i))
                {
                    break;
                }
            }

            qu_locked_node.push(shared_lock<shared_mutex>((((InternalNode *)p)->getChild(i)->getSharedMutex())));
            if (p != m_pRoot)
            {
                //拿到当前层结点锁弹出上上层结点
                qu_locked_node.pop();
                if (proot_guard.owns_lock())
                {
                    proot_guard.unlock();
                }
            }
            p = ((InternalNode *)p)->getChild(i);
        }
        if (p->getNodeType() == LEAF_NODE)
        {
            break;
        }
        first_key++;
    }

    while (p != NULL)
    {
        if (p->getKey(0) > high)
        {
            promise_ret->set_value(res);
            return res;
        }
        for (int i = 0; i < p->getKeyNum(); i++)
        {
            int key = ((LeafNode *)p)->getKey(i);
            if (key >= low && key <= high)
            {
                int ret = ((LeafNode *)p)->getValue(i);
                res.push_back(ret);
            }
        }
        p = ((LeafNode *)p)->getRightNode();
    }

    return res;
}