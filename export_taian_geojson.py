"""
导出 E:\data\泰安\BDC\DataSource.udbx 中的所有数据集为 GeoJSON
使用 os.add_dll_directory 方式正确加载 iObjectsPy
"""
import os, sys, json

DLL_DIR        = r"D:\software\supermap-iobjectspy-2025\lib\objectsjava\bin_win"
IOBJECTSPY_DIR = r"D:\software\supermap-iobjectspy-2025\iobjectspy\iobjectspy-py310_64"
UDBX_PATH      = r"E:\data\泰安\BDC\DataSource.udbx"
OUT_DIR        = r"C:\Users\jia\WorkBuddy\20260330192149\taian_geojson"

# 关键：修改 PATH 使 Java 子进程也能找到 DLL
os.environ["PATH"] = DLL_DIR + os.pathsep + os.environ.get("PATH", "")

# 显式添加 DLL 目录（影响 Python 侧的 DLL 加载）
os.add_dll_directory(DLL_DIR)

sys.path.insert(0, IOBJECTSPY_DIR)
import iobjectspy as spy
spy.set_iobjects_java_path(DLL_DIR)


os.makedirs(OUT_DIR, exist_ok=True)

DATASETS = ["范围", "ZRZ_bak", "NewRegion"]

results = {}
for ds_name in DATASETS:
    out_path = os.path.join(OUT_DIR, f"{ds_name}.geojson")
    print(f"导出 {ds_name} ...", flush=True)
    try:
        ret = spy.export_to_geojson(UDBX_PATH, ds_name, out_path)
        size = os.path.getsize(out_path) if os.path.exists(out_path) else 0
        results[ds_name] = {"ok": True, "path": out_path, "size_kb": round(size/1024, 1)}
        print(f"  OK {round(size/1024,1)} KB", flush=True)
    except Exception as e:
        results[ds_name] = {"ok": False, "error": str(e)}
        print(f"  FAIL: {e}", flush=True)

print("\n=== 导出汇总 ===", flush=True)
for k, v in results.items():
    if v["ok"]:
        print(f"  {k}: {v['size_kb']} KB -> {v['path']}", flush=True)
    else:
        print(f"  {k}: 失败 - {v['error']}", flush=True)

print("DONE", flush=True)
