"""
批量合并 GDB 数据并新增"区域"字段
本脚本可独立运行或在 MCP Server 进程内执行。

策略：
  1. 逐个 GDB 导入到临时 UDBX
  2. 在临时 UDBX 中为每个数据集新增"区域"字段并赋值
  3. 将临时数据追加（append）到最终合并数据源
"""
import sys, os, re, traceback

IOBJECTSPY_PATH = r"D:\software\supermap-iobjectspy-2025\iobjectspy\iobjectspy-py310_64"
JAVA_PATH       = r"D:\software\supermap-iobjectspy-2025\lib\objectsjava\bin_win"
DATA_DIR    = r"D:\BaiduNetdiskDownload\桌面GIS高级\GDB数据\data"
OUTPUT_UDBX = r"D:\BaiduNetdiskDownload\桌面GIS高级\GDB数据\data\merged_result.udbx"
TMP_UDBX    = r"D:\BaiduNetdiskDownload\桌面GIS高级\GDB数据\data\_tmp_region.udbx"

# 添加 iObjectsPy 到路径（如果还未加载）
if IOBJECTSPY_PATH not in sys.path:
    sys.path.insert(0, IOBJECTSPY_PATH)

import iobjectspy as spy

# 关键：调用 set_iobjects_java_path 初始化 Java 引擎（与 MCP Server 一致）
try:
    spy.set_iobjects_java_path(JAVA_PATH)
    print(f"Java 引擎初始化完成: {JAVA_PATH}", flush=True)
except Exception as e:
    print(f"Java 初始化警告（可能已初始化）: {e}", flush=True)

from iobjectspy import (
    DatasourceConnectionInfo, EngineType,
    create_datasource, open_datasource,
    FieldInfo, FieldType
)

def log(msg): print(msg, flush=True)

def open_udbx(path):
    conn = DatasourceConnectionInfo()
    conn.set_server(path)
    conn.set_type(EngineType.UDBX)
    return open_datasource(conn)

def create_udbx(path):
    if os.path.exists(path):
        try:
            os.remove(path)
        except:
            import time; time.sleep(1)
            try: os.remove(path)
            except: pass
    conn = DatasourceConnectionInfo()
    conn.set_server(path)
    conn.set_type(EngineType.UDBX)
    ds = create_datasource(conn)
    if ds:
        ds.close()

# ─── 扫描 GDB 列表 ──────────────────────────────────────────────────────────
gdb_list = []
for name in os.listdir(DATA_DIR):
    full = os.path.join(DATA_DIR, name)
    if name.endswith(".gdb") and os.path.isdir(full):
        region_name = name[:-4]
        gdb_list.append((region_name, full))

def region_num(item):
    m = re.search(r'\d+', item[0])
    return int(m.group()) if m else 999

gdb_list.sort(key=region_num)
log(f"找到 {len(gdb_list)} 个 GDB: {[x[0] for x in gdb_list]}")

# ─── 创建目标数据源 ─────────────────────────────────────────────────────────
create_udbx(OUTPUT_UDBX)
log(f"目标数据源创建完成: {os.path.basename(OUTPUT_UDBX)}")

# ─── 逐个 GDB 处理 ──────────────────────────────────────────────────────────
for idx, (region_name, gdb_path) in enumerate(gdb_list):
    log(f"\n{'='*50}")
    log(f"[{idx+1}/{len(gdb_list)}] 处理区域: {region_name}")

    # Step 1: 导入 GDB 到临时 UDBX
    create_udbx(TMP_UDBX)
    log(f"  正在导入 GDB...")
    try:
        result = spy.import_file_gdb_vector(gdb_path, TMP_UDBX)
        log(f"  导入完成: {result}")
    except Exception as e:
        log(f"  导入失败: {e}")
        log(traceback.format_exc())
        continue

    # Step 2: 打开临时数据源，新增区域字段并赋值
    tmp_ds = open_udbx(TMP_UDBX)
    if not tmp_ds:
        log(f"  ERROR: 无法打开临时数据源")
        continue

    for ds_item in tmp_ds.datasets:
        ds_name = ds_item.name
        cnt = ds_item.get_record_count()
        log(f"  处理数据集: {ds_name}  记录数: {cnt}")

        # 新增"区域"字段（如果不存在）
        existing_fields = [fi.name for fi in ds_item.field_infos]
        if "区域" not in existing_fields:
            fi = FieldInfo()
            fi.name = "区域"
            fi.type = FieldType.WTEXT
            fi.max_length = 50
            ok = ds_item.create_field(fi)
            log(f"    新增字段[区域]: {'成功' if ok else '失败'}")
        else:
            log(f"    字段[区域]已存在，直接赋值")

        # 批量赋值（所有记录赋值为 region_name）
        try:
            updated = ds_item.update_field("区域", region_name)
            log(f"    赋值[区域={region_name}]: {updated}")
        except Exception as e:
            log(f"    update_field 失败: {e}")
            log(traceback.format_exc())

    tmp_ds.close()

    # Step 3: 追加到目标数据源
    log(f"  追加到目标数据源...")
    tmp_ds2 = open_udbx(TMP_UDBX)
    tgt_ds  = open_udbx(OUTPUT_UDBX)

    if tmp_ds2 and tgt_ds:
        for src_ds_item in tmp_ds2.datasets:
            ds_name = src_ds_item.name
            tgt_ds_item = tgt_ds.get_dataset(ds_name)

            if tgt_ds_item is None:
                # 首次：复制整个数据集（含结构和数据）
                log(f"    首次创建数据集: {ds_name}")
                try:
                    new_ds = src_ds_item.copy_to(tgt_ds, ds_name)
                    if new_ds:
                        log(f"    复制成功，记录数: {new_ds.get_record_count()}")
                    else:
                        log(f"    复制失败!")
                except Exception as e:
                    log(f"    复制异常: {e}")
                    log(traceback.format_exc())
            else:
                # 追加数据
                log(f"    追加到已有数据集: {ds_name}")
                try:
                    r = tgt_ds_item.append(src_ds_item)
                    log(f"    追加结果: {r}  当前总记录数: {tgt_ds_item.get_record_count()}")
                except Exception as e:
                    log(f"    追加异常: {e}")
                    log(traceback.format_exc())
    else:
        log(f"  ERROR: 无法打开数据源进行追加操作")

    if tmp_ds2: tmp_ds2.close()
    if tgt_ds:  tgt_ds.close()

    # 清理临时文件
    if os.path.exists(TMP_UDBX):
        try:
            os.remove(TMP_UDBX)
            log(f"  清理临时文件完成")
        except Exception as e:
            log(f"  清理临时文件失败: {e}")

# ─── 最终验证 ────────────────────────────────────────────────────────────────
log(f"\n{'='*50}")
log("最终验证:")
final_ds = open_udbx(OUTPUT_UDBX)
if final_ds:
    for ds_item in final_ds.datasets:
        cnt = ds_item.get_record_count()
        # 查询区域字段的不同值
        try:
            vals = ds_item.get_field_values("区域")
            unique_regions = list(set(vals)) if vals else []
            unique_regions.sort()
        except Exception as e:
            unique_regions = [f"(查询失败: {e})"]
        log(f"  {ds_item.name}: {cnt} 条记录  区域值({len(unique_regions)}个): {unique_regions}")
    final_ds.close()
    log(f"\n✅ 合并完成！输出: {OUTPUT_UDBX}")
else:
    log("ERROR: 无法打开最终数据源")

print("SCRIPT_DONE")
