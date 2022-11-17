#include <iostream>
#include<vector>
#include<string.h>
#include "TableManager.h"
#include "Serializable.h"
#include "Test.h"
 
using namespace std;

int main(int argc, char *argv[])
{
	if(argc<2)
	{
		cout<<"params error."<<endl;
		cout<<"params: A ->B+树并发测试; "<<endl;
		cout<<"params: B ->整体测试; "<<endl;
		return 0;
	}
	Test test=Test();
	if(strcmp(argv[1],"A")==0)
	{
		test.testBPlusTree();
	}
	else if(strcmp(argv[1],"B")==0)
	{
		test.testAll();
	}
	

}