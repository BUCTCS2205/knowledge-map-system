import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

graph = GraphDatabase.driver("bolt://39.105.26.212:7687", auth=("neo4j", "neo4jgraph"))    
        
cql = '''
    MERGE (source:project {name: $pname})
    MERGE (target:people  {name: $wname})
    MERGE (source)-[:writer]->(target)
    '''        
csv_path = "作者.csv"
df = pd.read_csv(csv_path, encoding='utf-8')

with graph.session() as session:
    # for _, row in df.iterrows():
    for _, row in tqdm(df.iterrows(), total=len(df), desc="添加时间属性"):
        params = {
            "pname": row['主体'],
            "wname": row['对象'],
        }
        session.run(cql, params)
        # print(params)
            
print("导入完成！")


    
