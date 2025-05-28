#!/usr/bin/env python3
# merge_four_csvs.py

import pandas as pd


def merge_csv_files(output_path="merged_artifacts.csv"):
    # 1. 列出要合并的四个 CSV 文件
    files = [
        "chinese_artifacts_1.csv",
        "metmuseum_final.csv",
        "museum_artifact_details.csv",
        "Philamuseum_final.csv"
    ]

    # 2. 依次读取，遇到编码问题时使用 gb18030
    dfs = []
    for fp in files:
        try:
            df = pd.read_csv(fp, dtype=str, encoding="utf-8")
        except UnicodeDecodeError:
            df = pd.read_csv(fp, dtype=str, encoding="gb18030")
        dfs.append(df)
        print(f"已读取：{fp} （{len(df)} 条记录）")

    # 3. 按列名对齐，纵向合并
    merged = pd.concat(dfs, axis=0, ignore_index=True, sort=False)
    print(f"合并后总记录数：{len(merged)}，共 {len(merged.columns)} 列")

    # 4. 保存为新的 CSV，使用 utf-8-sig 以保证 Excel 打开时中文正常
    merged.to_csv(output_path, index=False, encoding="utf-8-sig")
    print(f"已生成合并文件：{output_path}")


if __name__ == "__main__":
    merge_csv_files()
