// #ifndef Serialization_H
// #define Serialization_H
// /*--------------序列化类------------*/
// #include<string>
// #include<iostream>

// using namespace std;

// //支持序列化类型
// enum TypeName
// {
//     VECTOR,
//     RECORD,
//     TABLEMANAGER
    
// };
// template<typename T>
// class Serializable
// {
//     private:
//     Serializable();
//     //获得类型
//     TypeName getType();
//     void serializationVector();
//     void serializationTableManager();
//     void serializationTableManager();


//     public:
//     //获得单例
//     static Serializable& getInstance()
//     {
//         static Serializable _instance;
//         return _instance;
//     }

//     //序列化
//     void SerializationToBinary();
    

// };

// template<typename T>
// TypeName Serializable<T>::getType()
// {
//     if(typeid(T)==typeid(vector<int64_t>))
//     {
//         return VECTOR;
//     }
//     else if (typeid(T) == typeid(TableManager))
//     {
//         return TABLEMANAGER;
//     }
//     else if(typeid(T)==typeid(Record))
//     {
//         return RECORD;
//     }
//     else
//     {
//         throw "不支持的序列化数据类型."
//     }
    
// }
// //序列化
// template<typename T>
// void Serializable<T>::SerializationToBinary()
// {
//     if()
// }

// #endif