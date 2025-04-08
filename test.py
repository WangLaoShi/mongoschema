from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

def test_mongodb_connection():
    try:
        # 创建MongoDB客户端连接（添加认证信息）
        client = MongoClient(
            'mongodb://192.168.0.5:27017',
            username='root',  # 替换为实际的用户名
            password='123456',    # 替换为实际的密码
            serverSelectionTimeoutMS=5000
        )
        
        # 测试连接
        client.server_info()
        print("✅ MongoDB连接成功！")
        
        # 获取数据库列表
        databases = client.list_database_names()
        print("\n可用的数据库:")
        for db in databases:
            print(f"- {db}")
            
        # 关闭连接
        client.close()
        
    except ConnectionFailure as e:
        print(f"❌ MongoDB连接失败: {e}")
    except Exception as e:
        print(f"❌ 发生错误: {e}")

if __name__ == "__main__":
    test_mongodb_connection()