import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# 数据库配置（根据实际情况修改）
DB_CONFIG = {
    "host": "39.105.26.212",
    "port": 3306,
    "user": "museumdb",
    "password": "123456",
    "database": "museumdb",
    "charset": "utf8mb4"
}

def create_mysql_table(engine):
    """创建藏品信息表（id主键）"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS artifacts (
        id INT AUTO_INCREMENT PRIMARY KEY,
        藏品名称 VARCHAR(255) NOT NULL,
        藏品来源 VARCHAR(255),
        年代 VARCHAR(1024),
        介绍 TEXT,
        图片链接 VARCHAR(512),
        详情链接 VARCHAR(512)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
    """
    try:
        with engine.connect() as conn:
            conn.execute(text(create_table_sql))
            conn.commit()
        print("数据表创建成功")
    except SQLAlchemyError as e:
        print(f"建表失败: {str(e)}")

def import_csv_to_mysql(csv_file):
    """执行CSV导入操作"""
    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        f"?charset={DB_CONFIG['charset']}"
    )
    try:
        create_mysql_table(engine)
        df = pd.read_csv(
            csv_file,
            encoding='utf-8-sig',
            usecols=['藏品名称', '藏品来源', '年代', '介绍', '图片链接', '详情链接'],
            dtype={'年代': str},
            na_filter=False
        )
        # 数据清洗
        df = df.where(pd.notnull(df), None)
        df['图片链接'] = df['图片链接'].apply(
            lambda x: x if isinstance(x, str) and x.startswith('http') else None
        )
        # 批量导入数据（允许重复）
        df.to_sql(
            name='artifacts',
            con=engine,
            if_exists='append',
            index=False,
            method='multi',
            chunksize=1000
        )
        print(f"成功导入 {len(df)} 条记录")
        # 验证数据
        with engine.connect() as conn:
            result = conn.execute(text("SELECT COUNT(*) FROM artifacts")).scalar()
            print(f"当前数据库总记录数: {result}")

    except SQLAlchemyError as e:
        print(f"数据库操作失败: {str(e)}")
    except Exception as e:
        print(f"发生错误: {str(e)}")
    finally:
        engine.dispose()

if __name__ == "__main__":
    import_csv_to_mysql('combined.csv')