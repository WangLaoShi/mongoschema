from mongoschema import SchemaAnalyzer

def analyze_mongodb_schema():
    try:
        # 创建SchemaAnalyzer实例
        # 使用与之前相同的连接信息
        schema = SchemaAnalyzer(
            host='mongodb://localhost:27017',
            username='root',
            password='root',
            db='database',  # 你想分析的数据库名
            collection='coll'  # 你想分析的集合名
        )
        
        # 分析schema
        schema.analyze()
        
        # 打印分析结果
        print("\n数据库结构分析结果:")
        print(schema)  # 这会调用__str__方法，显示格式化的结果
        
        # 可选：导出到CSV文件
        schema.to_csv('schema_report.csv')
        print("\n分析结果已导出到 schema_report.csv")
        
    except Exception as e:
        print(f"❌ 分析过程中发生错误: {e}")
    finally:
        # 确保关闭连接
        if hasattr(schema, 'cursor'):
            schema.cursor.close()

if __name__ == "__main__":
    analyze_mongodb_schema()