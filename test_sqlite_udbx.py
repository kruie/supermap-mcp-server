"""
尝试用 sqlite3 直接读取 UDBX（UDBX 是 SQLite-based 文件）
"""
import sqlite3, sys

try:
    conn = sqlite3.connect(r'E:\data\泰安\BDC\DataSource.udbx')
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cur.fetchall()
    print('Tables in UDBX:')
    for t in tables:
        print(' ', t[0])
    conn.close()
except Exception as e:
    print(f'Error: {e}')
