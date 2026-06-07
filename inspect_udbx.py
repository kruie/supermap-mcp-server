"""
直接通过 sqlite3 + spatialite 读取 UDBX 数据（UDBX = SpatiaLite SQLite）
"""
import sqlite3, json, sys, os

UDBX_PATH = r'E:\data\泰安\BDC\DataSource.udbx'
OUT_DIR   = r'C:\Users\jia\WorkBuddy\20260330192149\taian_geojson'

os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(UDBX_PATH)
cur = conn.cursor()

# 先查看表结构
for tbl in ['范围', 'ZRZ_bak', 'NewRegion']:
    print(f"\n=== 表: {tbl} ===")
    try:
        cur.execute(f"PRAGMA table_info([{tbl}])")
        cols = cur.fetchall()
        for c in cols:
            print(f"  {c[1]:30s} {c[2]}")
        cur.execute(f"SELECT COUNT(*) FROM [{tbl}]")
        cnt = cur.fetchone()[0]
        print(f"  记录数: {cnt}")
    except Exception as e:
        print(f"  ERROR: {e}")

conn.close()
