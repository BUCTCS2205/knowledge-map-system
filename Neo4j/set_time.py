import pandas as pd
from neo4j import GraphDatabase
from tqdm import tqdm

graph = GraphDatabase.driver("bolt://39.105.26.212:7687", auth=("neo4j", "neo4jgraph"))    
        
cql = '''
    MERGE (e:project {name: $name}) 
    ON CREATE SET e.time = $time  
    ON MATCH SET e.time = $time  
    '''        
csv_path = "年代.csv"
df = pd.read_csv(csv_path, encoding='utf-8')

with graph.session() as session:
    # for _, row in df.iterrows():
    for _, row in tqdm(df.iterrows(), total=len(df), desc="添加时间属性"):
        params = {
            "name": row['主体'],
            "time":row['年代'],
        }
        session.run(cql, params)
        # print(params)
            
print("导入完成！")


    
