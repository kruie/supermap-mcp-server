"""探查 GDB 内的要素类名称"""
import sys
import os

iobjectspy_path = r"D:/software/supermap-iobjectspy-2025/iobjectspy/iobjectspy-py310_64"
java_path = r"D:/software/supermap-iobjectspy-2025/lib/objectsjava/bin_win"
os.environ["SUPERMAP_HOME"] = r"D:/software/supermap-iobjectspy-2025"
os.environ["PATH"] = java_path + os.pathsep + os.environ.get("PATH", "")

sys.path.insert(0, iobjectspy_path)
import iobjectspy as ioby

gdb_path = r"D:\BaiduNetdiskDownload\桌面GIS高级\GDB数据\data\第1区域.gdb"

from iobjectspy import *

ws = Workspace()
conn = DatasourceConnectionInfo()
conn.server = gdb_path
conn.engine_type = EngineType.FILEGDBE

ds = ws.datasources.open(conn)
if ds:
    print(f"数据源打开成功，数据集数量: {ds.datasets.count}")
    for i in range(ds.datasets.count):
        dt = ds.datasets[i]
        print(f"  [{i}] 名称: {dt.name}  类型: {dt.type}")
    ds.close()
else:
    print("数据源打开失败")

ws.close()
