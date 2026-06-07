"""
直接通过 sqlite3 + shapely 读取 UDBX (SpatiaLite) 导出 GeoJSON
UDBX 的几何列是 SpatiaLite WKB，需要去掉前4字节（SRID）后才是标准 WKB
"""
import sqlite3, json, os, sys
from shapely import wkb
from shapely.geometry import mapping

UDBX_PATH = r'E:\data\泰安\BDC\DataSource.udbx'
OUT_DIR   = r'C:\Users\jia\WorkBuddy\20260330192149\taian_geojson'
os.makedirs(OUT_DIR, exist_ok=True)

conn = sqlite3.connect(UDBX_PATH)
cur  = conn.cursor()

DATASETS = {
    "fan_wei": "范围",          # 范围（边界）
    "ZRZ_bak": "ZRZ_bak",       # 主数据 5026 条
    "NewRegion": "NewRegion",   # 新区域
}

def spatialite_blob_to_wkb(blob):
    """
    SpatiaLite Blob 格式:
    [0]   : start marker (0x00)
    [1]   : byte order (0x01 = little endian)
    [2-5] : SRID (4 bytes)
    [6-37]: MBR (32 bytes)
    [38]  : end header marker (0x7C)
    [39+] : WKB geometry
    """
    if blob is None:
        return None
    data = bytes(blob)
    # 找 0x7C 标记
    idx = data.find(b'\x7c')
    if idx == -1:
        return None
    return data[idx+1:]

def table_to_geojson(table_name, out_name):
    print(f"导出: {table_name} ...", flush=True)
    
    # 获取所有列（排除系统内部列）
    cur.execute(f"PRAGMA table_info([{table_name}])")
    cols_info = cur.fetchall()
    all_cols  = [c[1] for c in cols_info]
    geom_col  = "SmGeometry"
    skip_cols = {"SmGeoParam"}   # 跳过 BLOB 系统列
    attr_cols = [c for c in all_cols if c not in skip_cols and c != geom_col]
    
    col_expr  = ", ".join([f"[{c}]" for c in attr_cols] + [f"[{geom_col}]"])
    cur.execute(f"SELECT {col_expr} FROM [{table_name}]")
    rows = cur.fetchall()
    
    features = []
    geom_idx = len(attr_cols)
    ok_count = 0
    fail_count = 0
    
    for row in rows:
        blob = row[geom_idx]
        wkb_data = spatialite_blob_to_wkb(blob)
        if wkb_data is None:
            fail_count += 1
            continue
        
        try:
            geom = wkb.loads(wkb_data)
            geom_dict = mapping(geom)
            
            props = {}
            for i, col in enumerate(attr_cols):
                val = row[i]
                if isinstance(val, (int, float, str, type(None))):
                    props[col] = val
                else:
                    props[col] = str(val)
            
            features.append({
                "type": "Feature",
                "geometry": geom_dict,
                "properties": props
            })
            ok_count += 1
        except Exception as e:
            fail_count += 1
    
    gj = {
        "type": "FeatureCollection",
        "name": out_name,
        "crs": {
            "type": "name",
            "properties": {"name": "urn:ogc:def:crs:OGC:1.3:CRS84"}
        },
        "features": features
    }
    
    out_path = os.path.join(OUT_DIR, f"{out_name}.geojson")
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(gj, f, ensure_ascii=False)
    
    size_mb = os.path.getsize(out_path) / 1024 / 1024
    print(f"  OK: {ok_count} 个要素，{size_mb:.2f} MB -> {out_path}", flush=True)
    if fail_count:
        print(f"  WARN: {fail_count} 个几何解析失败", flush=True)
    return out_path, ok_count

results = {}
for out_name, tbl_name in DATASETS.items():
    try:
        path, cnt = table_to_geojson(tbl_name, out_name)
        results[tbl_name] = {"ok": True, "path": path, "count": cnt}
    except Exception as e:
        print(f"  ERROR: {e}", flush=True)
        results[tbl_name] = {"ok": False, "error": str(e)}

conn.close()

print("\n=== 完成 ===")
for k, v in results.items():
    if v["ok"]:
        print(f"  {k}: {v['count']} 条 -> {v['path']}")
    else:
        print(f"  {k}: FAIL - {v['error']}")
print("DONE")
