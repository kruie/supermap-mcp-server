"""
将 DataSource.udbx 中的数据集导出为 GeoJSON（在 MCP 进程内运行）
"""
import json, os

UDBX_PATH  = r"E:\data\泰安\BDC\DataSource.udbx"
OUT_DIR    = r"C:\Users\jia\WorkBuddy\20260330192149\taian_geojson"
DATASETS   = ["范围", "ZRZ_bak", "NewRegion"]

os.makedirs(OUT_DIR, exist_ok=True)

import iobjectspy as spy

def open_ds(path):
    conn = spy.DatasourceConnectionInfo()
    conn.set_server(path)
    conn.set_type(spy.EngineType.UDBX)
    ws = spy.Workspace()
    ds = ws.datasources.open(conn)
    return ws, ds

def dataset_to_geojson(ds, name):
    dv = ds[name]
    if dv is None:
        print(f"  [WARN] 数据集 {name} 不存在")
        return None
    
    features = []
    rs = dv.get_recordset(False, spy.CursorType.STATIC)
    if rs is None:
        print(f"  [WARN] 无法获取 {name} 的 Recordset")
        return None
    
    rs.move_first()
    fields = [rs.get_field_infos().get(i).get_name() for i in range(rs.get_field_infos().get_count())]
    
    count = 0
    while not rs.is_EOF():
        geom = rs.get_geometry()
        if geom:
            # 转为 GeoJSON 几何
            geom_json = geom.to_geojson() if hasattr(geom, 'to_geojson') else None
            
            if geom_json is None:
                # 手动构建（面要素）
                try:
                    pts = geom.get_points() if hasattr(geom, 'get_points') else None
                    if pts and hasattr(pts, '__len__'):
                        coords = [[pt.x, pt.y] for pt in pts]
                        if coords and coords[0] != coords[-1]:
                            coords.append(coords[0])
                        geom_json = json.dumps({"type": "Polygon", "coordinates": [coords]})
                except Exception as e:
                    pass
            
            if geom_json:
                if isinstance(geom_json, str):
                    geom_dict = json.loads(geom_json)
                else:
                    geom_dict = geom_json
                
                # 属性
                props = {}
                for f in fields:
                    try:
                        val = rs.get_field_value(f)
                        if hasattr(val, '__class__') and 'java' in str(type(val)):
                            val = str(val)
                        props[f] = val
                    except:
                        props[f] = None
                
                features.append({
                    "type": "Feature",
                    "geometry": geom_dict,
                    "properties": props
                })
                count += 1
        
        rs.move_next()
    
    rs.close()
    print(f"  {name}: {count} 个要素已导出")
    
    return {
        "type": "FeatureCollection",
        "name": name,
        "features": features
    }

print("开始导出 GeoJSON...", flush=True)
ws, ds = open_ds(UDBX_PATH)

results = {}
for name in DATASETS:
    print(f"处理: {name}", flush=True)
    gj = dataset_to_geojson(ds, name)
    if gj:
        out_path = os.path.join(OUT_DIR, f"{name}.geojson")
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(gj, f, ensure_ascii=False, indent=2)
        results[name] = {"path": out_path, "count": len(gj["features"])}
        print(f"  -> 已保存: {out_path}", flush=True)

ws.close()
print(f"\n完成！导出文件列表:", flush=True)
for k, v in results.items():
    print(f"  {k}: {v['count']} 个要素 -> {v['path']}", flush=True)
print("DONE", flush=True)
