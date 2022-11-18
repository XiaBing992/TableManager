#include "BPlusTree.h"
#include "TableManager.h"
class Test
{
    private:
    Logger& logger=Logger::getInstance();

    /*-------------------b+树插入线程测试函数------------------*/
    void bPlusTreeInsertTest(BPlusTree* bPlusTree,int start,int end);
    void bPlusTreeInsertTestByRand(BPlusTree* bPlusTree,int n);

    /*-------------------B+树删除线程测试函数------------------*/
    void bPlusTreeDeleteTest(BPlusTree* bPlusTree,int start,int end);
    void bPlusTreeDeleteTestByRand(BPlusTree* bPlusTree,int n);

    /*-------------------B+树查找线程测试函数------------------*/
    void bPlusTreeSearchTest(BPlusTree* bPlusTree,int start,int end);
    void bPlusTreeSearchTestByRand(BPlusTree* bPlusTree,int n);

    /*-------------------B+树修改线程测试函数------------------*/
    void bPlusTreeUpdateTest(BPlusTree* bPlusTree,int start,int end);
    void bPlusTreeUpdateTestByRand(BPlusTree* bPlusTree,int n);

    //多线程添加
    void apendTest(int n);

    public:
    //单元测试B+树
    void testBPlusTree();
    //整体测试
    void testAll();


};
