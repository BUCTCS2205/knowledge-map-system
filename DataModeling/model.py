#!/usr/bin/env python3
# export_triples_no_prefix.py
# 本脚本从 merged_artifacts.csv 读取数据，生成“作者”三列三元组和其他属性两列文件，
# 并且去掉主体前的 “文物/” 前缀，只保留清洗后的藏品名称。

import pandas as pd
import os

def main():
    # 1. 读取合并好的 CSV（与脚本同目录）
    df = pd.read_csv("merged_artifacts.csv", dtype=str, encoding="utf-8-sig")
    # 2. 输出目录
    out_dir = "三元组"
    os.makedirs(out_dir, exist_ok=True)
    # 3. 处理“作者”——实体–关系–实体，三列格式
    if "作者" in df.columns:
        triples = []
        for _, row in df.iterrows():
            名称 = row.get("藏品名称")
            作者 = row.get("作者")
            if pd.isna(名称) or pd.isna(作者):
                continue
            # 清洗名称和作者文本
            名称_clean = 名称.strip().replace(' ', '_').replace('/', '-')
            作者_clean = 作者.strip().replace(' ', '_').replace('/', '-')
            # 去掉“文物/”前缀，直接用清洗后的名称作为主体
            subj = 名称_clean
            pred = "作者"
            obj  = f"{作者_clean}"
            triples.append((subj, pred, obj))
        # 保存 author.csv
        author_df = pd.DataFrame(triples, columns=["主体", "关系", "对象"])
        author_df.to_csv(os.path.join(out_dir, "作者.csv"),
                         index=False, encoding="utf-8-sig")
        print(f"✔ 已生成 作者.csv （共 {len(triples)} 条）")
    # 4. 处理属性——实体–属性，二列格式
    属性列表 = ["年代", "介绍", "图片链接", "详情链接"]
    for col in 属性列表:
        if col not in df.columns:
            continue
        records = []
        for _, row in df.iterrows():
            名称 = row.get("藏品名称")
            值   = row.get(col)
            if pd.isna(名称) or pd.isna(值):
                continue
            名称_clean = 名称.strip().replace(' ', '_').replace('/', '-')
            # 去掉“文物/”前缀，直接用清洗后的名称作为主体
            records.append((名称_clean, 值.strip()))
        # 保存每个属性的 CSV，列名：主体, 属性名
        out_path = os.path.join(out_dir, f"{col}.csv")
        out_df = pd.DataFrame(records, columns=["主体", col])
        out_df.to_csv(out_path, index=False, encoding="utf-8-sig")
        print(f"✔ 已生成 {col}.csv （共 {len(records)} 条）")

if __name__ == "__main__":
    main()
