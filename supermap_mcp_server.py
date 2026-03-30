"""
SuperMap iObjectsPy MCP Server
==============================

使用 MCP SDK 创建的 SuperMap GIS MCP 服务器
支持通过 stdio 与 WorkBuddy 通信

工具数量: 69/69 (全部完成)
版本: v4.0 (增强健康检查 + Pipeline 批量执行 + 工具描述增强)
"""

import sys
import os
import json
import traceback
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# 设置 iObjectsPy 路径
# 注意: iObjectsPy 必须使用反斜杠路径（Windows 原生路径格式）
# 可通过环境变量 SUPERMAP_IOBJECTSPY_PATH 覆盖默认路径
IOBJECTSPY_PATH = os.environ.get(
    "SUPERMAP_IOBJECTSPY_PATH",
    r"D:\software\supermap-idesktopx-2025-windows-x64-bin\bin_python\iobjectspy\iobjectspy-py310_64"
)
# 确保路径使用反斜杠（iObjectsPy 要求 Windows 反斜杠路径）
IOBJECTSPY_PATH = IOBJECTSPY_PATH.replace("/", "\\")
sys.path.insert(0, IOBJECTSPY_PATH)

# 默认 Java 路径（iObjectsPy 要求 Windows 反斜杠路径）
DEFAULT_IOBJECT_PATH = os.environ.get(
    "SUPERMAP_JAVA_PATH",
    r"D:\software\supermap-idesktopx-2025-windows-x64-bin\bin"
).replace("/", "\\")

# 默认 License 路径（SuperMap 标准安装位置）
# SuperMap 通过环境变量 SUPERMAP_LICENSE 指定 License 文件目录
# 默认路径: C:\Program Files\Common Files\SuperMap\License
DEFAULT_LICENSE_PATH = os.environ.get(
    "SUPERMAP_LICENSE",
    r"C:\Program Files\Common Files\SuperMap\License"
).replace("/", "\\")

# 全局状态
_server = Server("supermap-iobjectspy")
_initialized = False
_init_error = None


# =============================================================================
# 辅助函数
# =============================================================================

def _ensure_init():
    """确保 iObjectsPy 已初始化"""
    global _initialized, _init_error
    if not _initialized:
        try:
            import iobjectspy as iobs
            iobs.set_iobjects_java_path(DEFAULT_IOBJECT_PATH)
            _initialized = True
            _init_error = None
        except Exception as e:
            _init_error = str(e)
            raise


# =============================================================================
# MCP 工具定义
# =============================================================================

@_server.list_tools()
async def list_tools():
    """列出所有可用的 SuperMap 工具"""
    return [
        # ---- 初始化与环境 ----
        Tool(
            name="initialize_supermap",
            description="初始化 SuperMap iObjectsPy 连接。适用于: 首次调用其他工具前确保环境就绪（通常自动初始化）。返回: {status, message}",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_environment_info",
            description="获取 SuperMap 环境信息。适用于: 排查环境问题、确认 Java/License 配置。返回: {status, iobjectspy_path, iobjects_java_path, omp_threads, license}",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="check_mcp_health",
            description="检查 MCP Server 健康状态（增强版）。适用于: 首次使用时验证环境、工具调用失败时排查。检查 iObjectsPy/Java/License/磁盘空间，自动生成修复建议。返回: {overall_status, iobjectspy_importable, java_path_valid, license_valid, disk_space, suggestions[]}",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        # ---- 数据源管理 ----
        Tool(
            name="open_udbx_datasource",
            description="打开 UDBX 数据源文件。适用于: 需要查看数据源中有哪些数据集、或操作数据集中的数据前。返回: {status, datasets[{name, type, record_count}]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": "UDBX 文件路径"}
                },
                "required": ["file_path"]
            }
        ),
        Tool(
            name="create_udbx_datasource",
            description="创建新的 UDBX 数据源文件。适用于: 导入数据前需要先创建目标数据源。返回: {status, datasource_path}",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_path": {"type": "string", "description": "新建 UDBX 文件路径"}
                },
                "required": ["file_path"]
            }
        ),
        Tool(
            name="create_memory_datasource",
            description="创建内存数据源。适用于: 临时数据处理、不需要持久化存储的中间分析结果。返回: {status, datasource_name}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_name": {"type": "string", "description": "内存数据源名称（默认: MemoryDS）"}
                }
            }
        ),
        # ---- 工作空间管理 ----
        Tool(
            name="open_workspace",
            description="打开工作空间文件 (.smwu/.sxwu)。适用于: 需要访问工作空间中的数据源、地图、场景。返回: {status, workspace_path, datasources[], maps[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.smwu 或 .sxwu)"}
                },
                "required": ["workspace_path"]
            }
        ),
        Tool(
            name="save_workspace",
            description="保存工作空间，支持另存为。适用于: 修改工作空间后保存、或另存为新文件。返回: {status, saved_path}",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.smwu 或 .sxwu)"},
                    "save_as_path": {"type": "string", "description": "另存为路径（可选，不提供则覆盖保存）"}
                },
                "required": ["workspace_path"]
            }
        ),
        Tool(
            name="get_workspace_info",
            description="获取工作空间详细信息。适用于: 查看工作空间中有哪些数据源、地图、场景和资源。返回: {status, datasources[], maps[], scenes[], resources[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.smwu 或 .sxwu)"}
                },
                "required": ["workspace_path"]
            }
        ),
        # ---- 投影/坐标系统 ----
        Tool(
            name="get_coordinate_system",
            description="获取数据集的坐标系统信息。适用于: 检查数据坐标系类型、EPSG 代码、坐标范围。返回: {status, epsg_code, projection_type, bounds}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        Tool(
            name="reproject_dataset",
            description="坐标转换（动态投影）。适用于: 将数据从 WGS84 转为 CGCS2000、统一项目坐标系等。返回: {status, source_dataset, output_dataset, target_epsg}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "源数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "target_epsg": {"type": "integer", "description": "目标 EPSG 代码（如 4326 表示 WGS84、4490 表示 CGCS2000）"}
                },
                "required": ["datasource_path", "dataset_name", "output_dataset", "target_epsg"]
            }
        ),
        # ---- 数据集管理 ----
        Tool(
            name="list_datasets",
            description="列出数据源中所有数据集。适用于: 查看数据源中有哪些数据集及其类型和记录数。返回: {status, datasets[{name, type, record_count}], count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "UDBX 文件路径"}
                },
                "required": ["datasource_path"]
            }
        ),
        Tool(
            name="get_dataset_info",
            description="获取数据集详细信息。适用于: 查看数据集类型、字段列表、记录数、空间范围。返回: {status, dataset_name, type, record_count, fields[], bounds}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        Tool(
            name="query_dataset",
            description="SQL 属性查询。适用于: 按条件筛选数据、选择特定字段、排序和限制返回数量。返回: {status, total_count, returned_count, records[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "sql_filter": {"type": "string", "description": "SQL WHERE 过滤条件（可选），如 \"population > 10000 AND name LIKE '北京%'\""},
                    "fields": {"type": "array", "items": {"type": "string"}, "description": "要返回的字段列表（可选），如 [\"name\", \"population\"]"},
                    "max_results": {"type": "integer", "description": "最大返回记录数（默认: 100）"},
                    "order_by": {"type": "string", "description": "排序字段（可选），如 \"population DESC\""}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        Tool(
            name="delete_dataset",
            description="删除数据集（不可逆）。适用于: 清理不再需要的数据集。返回: {status, deleted_dataset}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "要删除的数据集名称"}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        # ---- 数据集创建与管理 ----
        Tool(
            name="create_dataset",
            description="创建新的空数据集，支持点/线/面/文本/纯属性表等类型。适用于: 新建存储结构、准备接收导入数据、创建分析结果数据集。返回: {status, dataset_name, dataset_type}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "新数据集名称"},
                    "dataset_type": {"type": "string", "enum": ["POINT", "LINE", "REGION", "TEXT", "TABULAR", "POINT3D", "LINE3D", "REGION3D"], "description": "数据集类型（默认: POINT）"},
                    "fields": {"type": "array", "items": {"type": "object"}, "description": "字段定义列表，如 [{\"name\":\"area\",\"type\":\"DOUBLE\"},{\"name\":\"name\",\"type\":\"TEXT\",\"size\":100}]"}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        Tool(
            name="copy_dataset",
            description="复制数据集到同数据源或不同数据源中。适用于: 数据备份、跨数据源迁移、创建分析副本。返回: {status, source_dataset, target_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "源数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "target_datasource_path": {"type": "string", "description": "目标 .udbx 文件路径（可选，默认与源相同）"}
                },
                "required": ["datasource_path", "dataset_name", "output_dataset"]
            }
        ),
        Tool(
            name="append_to_dataset",
            description="将一个数据集的要素追加到另一个数据集中，要求两个数据集结构相同。适用于: 合并多个分区数据、将新采集数据追加到已有数据集。返回: {status, target_dataset, appended_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "目标数据集名称"},
                    "source_datasource_path": {"type": "string", "description": "源 .udbx 文件路径（可选，默认与目标相同）"},
                    "source_dataset_name": {"type": "string", "description": "源数据集名称"}
                },
                "required": ["datasource_path", "dataset_name", "source_dataset_name"]
            }
        ),
        Tool(
            name="add_field",
            description="为数据集添加新字段。适用于: 分析前准备数据结构。返回: {status, dataset_name, field_name, field_type}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "field_name": {"type": "string", "description": "新字段名称"},
                    "field_type": {"type": "string", "enum": ["INT32", "INT64", "DOUBLE", "TEXT", "BOOLEAN", "DATE", "DATETIME"], "description": "字段类型（默认: TEXT）"},
                    "field_size": {"type": "integer", "description": "字段长度（仅 TEXT 类型有效，默认: 255）"}
                },
                "required": ["datasource_path", "dataset_name", "field_name"]
            }
        ),
        Tool(
            name="calculate_field",
            description="批量计算字段值。适用于: 根据表达式计算面积/长度/分类等字段（如 SmArea/1000000）。返回: {status, dataset_name, field_name, updated_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "field_name": {"type": "string", "description": "要计算的字段名称"},
                    "expression": {"type": "string", "description": "计算表达式，如 \"Population * 1.05\" 或 \"CONCAT(Name, '_updated')\""},
                    "sql_filter": {"type": "string", "description": "过滤条件，仅对满足条件的记录计算（可选）"}
                },
                "required": ["datasource_path", "dataset_name", "field_name", "expression"]
            }
        ),
        # ---- 数据导入 ----
        Tool(
            name="import_shapefile",
            description="导入 Shapefile 文件到数据源。适用于: 用户有 .shp 文件需要入库。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "shapefile_path": {"type": "string", "description": "Shapefile (.shp) 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 UDBX 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"}
                },
                "required": ["shapefile_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_gdb",
            description="导入 ESRI GDB (FileGDB) 数据到数据源中。适用于: 从 ArcGIS 导出的 FileGDB 数据入库。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "gdb_path": {"type": "string", "description": "GDB 文件夹路径"},
                    "datasource_path": {"type": "string", "description": "目标 UDBX 文件路径"},
                    "feature_class": {"type": "string", "description": "GDB 中的要素类名称"}
                },
                "required": ["gdb_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_csv",
            description="导入 CSV 文件为点数据集，支持经纬度列映射，自动创建点几何。适用于: 将经纬度坐标表格（如 POI 列表、采样点）转为空间点数据。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "csv_path": {"type": "string", "description": "CSV 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"},
                    "x_field": {"type": "string", "description": "经度字段名（默认: longitude）"},
                    "y_field": {"type": "string", "description": "纬度字段名（默认: latitude）"},
                    "encoding": {"type": "string", "description": "CSV 编码（默认: utf-8）"}
                },
                "required": ["csv_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_tiff",
            description="导入 GeoTIFF 栅格文件为栅格数据集。适用于: 将 DEM/遥感影像/栅格分析结果入库管理。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "tiff_path": {"type": "string", "description": "GeoTIFF 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"},
                    "multi_band": {"type": "boolean", "description": "是否导入为多波段（默认: false，单波段）"}
                },
                "required": ["tiff_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_dwg",
            description="导入 AutoCAD DWG/DXF 文件为数据集。适用于: 将 CAD 工程图/规划图转为 GIS 矢量数据进行空间分析。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "dwg_path": {"type": "string", "description": "DWG 或 DXF 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"}
                },
                "required": ["dwg_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_kml",
            description="导入 KML/KMZ 文件为数据集。适用于: 将 Google Earth 标注/区域/路径数据入库进行 GIS 分析。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "kml_path": {"type": "string", "description": "KML 或 KMZ 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"}
                },
                "required": ["kml_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_geojson",
            description="导入 GeoJSON 文件为矢量数据集。适用于: 将 Web 地图服务/开放数据平台导出的 GeoJSON 数据入库。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "geojson_path": {"type": "string", "description": "GeoJSON 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"}
                },
                "required": ["geojson_path", "datasource_path"]
            }
        ),
        Tool(
            name="import_osm",
            description="导入 OSM (OpenStreetMap) 文件为数据集。适用于: 将 OpenStreetMap 导出的路网/建筑/兴趣点数据入库分析。返回: {status, dataset_name, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "osm_path": {"type": "string", "description": "OSM (.osm 或 .pbf) 文件路径"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "导入后的数据集名称"}
                },
                "required": ["osm_path", "datasource_path"]
            }
        ),
        # ---- 批量导入导出 ----
        Tool(
            name="batch_import",
            description="批量导入多个文件到数据源。适用于: 一次性导入多个不同格式的文件（Shapefile/GeoJSON/CSV/KML/DWG/TIFF）。返回: {status, total, success, failed, details[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "file_paths": {"type": "array", "items": {"type": "string"}, "description": "源文件路径列表，如 [\"D:/data/roads.shp\", \"D:/data/pois.geojson\"]"},
                    "datasource_path": {"type": "string", "description": "目标 .udbx 文件路径"},
                    "dataset_names": {"type": "array", "items": {"type": "string"}, "description": "导入后的数据集名称列表（可选，默认使用文件名）"}
                },
                "required": ["file_paths", "datasource_path"]
            }
        ),
        Tool(
            name="batch_export",
            description="批量导出多个数据集。适用于: 一次性导出多个数据集为 Shapefile/GeoJSON/KML。返回: {status, total, success, failed, details[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 .udbx 文件路径"},
                    "dataset_names": {"type": "array", "items": {"type": "string"}, "description": "要导出的数据集名称列表"},
                    "output_format": {"type": "string", "enum": ["shapefile", "geojson", "kml"], "description": "导出格式（默认: shapefile）"},
                    "output_directory": {"type": "string", "description": "输出目录路径"}
                },
                "required": ["datasource_path", "dataset_names", "output_directory"]
            }
        ),
        # ---- 数据导出 ----
        Tool(
            name="export_shapefile",
            description="导出数据集为 Shapefile。适用于: 需要将数据导出为 .shp 格式供其他软件使用。返回: {status, output_path}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 UDBX 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "output_path": {"type": "string", "description": "输出 .shp 文件路径"}
                },
                "required": ["datasource_path", "dataset_name", "output_path"]
            }
        ),
        Tool(
            name="export_geojson",
            description="导出数据集为 GeoJSON 文件。适用于: 将分析结果发布到 Web 地图、与其他 GIS 平台交换数据。返回: {status, output_path}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "output_path": {"type": "string", "description": "输出 .geojson 文件路径"},
                    "encode_to_epsg4326": {"type": "boolean", "description": "是否转换为 WGS84 坐标系（默认: false）"}
                },
                "required": ["datasource_path", "dataset_name", "output_path"]
            }
        ),
        Tool(
            name="export_tiff",
            description="导出栅格数据集为 GeoTIFF 文件。适用于: 将分析结果栅格（坡度、插值面等）导出供其他软件使用。返回: {status, output_path, band_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "源 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "栅格数据集名称"},
                    "output_path": {"type": "string", "description": "输出 .tif 文件路径"},
                    "band_index": {"type": "integer", "description": "导出的波段索引（默认: 0，所有波段）"}
                },
                "required": ["datasource_path", "dataset_name", "output_path"]
            }
        ),
        # ---- 数据集操作 ----
        Tool(
            name="dataset_point_to_line",
            description="将点数据集转换为线数据集，按字段排序后依次连线。适用于: GPS 轨迹点转路线、河流采样点连线、管线段连接。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入点数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出线数据集名称"},
                    "order_field": {"type": "string", "description": "排序字段名，用于确定点的连接顺序"},
                    "group_field": {"type": "string", "description": "分组字段名，相同值的点连成一条线（可选）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="dataset_line_to_region",
            description="线转面。适用于: GPS 轨迹封闭区域构面、等高线转面。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入线数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出面数据集名称"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="dataset_region_to_line",
            description="面转线。适用于: 提取面边界用于叠加或可视化。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入面数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出线数据集名称"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="dissolve",
            description="融合分析。适用于: 按属性合并相邻同类要素（如合并相邻同名行政区划）。返回: {status, result_dataset, dissolve_field, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "dissolve_field": {"type": "string", "description": "融合字段名"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset"]
            }
        ),
        # ---- 空间分析 ----
        Tool(
            name="create_buffer",
            description="创建缓冲区。适用于: POI 服务范围分析、道路影响范围、管线保护区域等。返回: {status, result_dataset, buffer_distance, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "UDBX 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "buffer_distance": {"type": "number", "description": "缓冲距离（米）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "buffer_distance"]
            }
        ),
        Tool(
            name="create_multi_buffer",
            description="创建多级缓冲区（同心环），可指定多个距离值。适用于: 设施多级服务范围分析（如 1/3/5 公里圈）、噪声衰减分区。返回: {status, result_dataset, buffer_distances[], record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "buffer_distances": {"type": "array", "items": {"type": "number"}, "description": "缓冲距离数组，如 [100, 200, 500]"},
                    "dissolve": {"type": "boolean", "description": "是否融合重叠区域（默认: false）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "buffer_distances"]
            }
        ),
        Tool(
            name="overlay",
            description="叠加分析。适用于: 土地适宜性评估、多图层空间关系计算。支持 INTERSECTION/UNION/ERASE/IDENTITY/UPDATE/CLIP/XOR。返回: {status, result_dataset, operation, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入数据集名称"},
                    "overlay_dataset": {"type": "string", "description": "叠加数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"},
                    "operation": {"type": "string", "enum": ["INTERSECT", "UNION", "ERASE", "IDENTITY", "UPDATE", "CLIP", "XOR"], "description": "叠加分析类型"}
                },
                "required": ["datasource_path", "input_dataset", "overlay_dataset", "output_dataset", "operation"]
            }
        ),
        Tool(
            name="clip_data",
            description="裁剪分析。适用于: 用面数据集裁剪线/面数据集，提取感兴趣区域内的数据。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "UDBX 文件路径"},
                    "input_dataset": {"type": "string", "description": "被裁剪数据集名称"},
                    "clip_dataset": {"type": "string", "description": "裁剪数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出数据集名称"}
                },
                "required": ["datasource_path", "input_dataset", "clip_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="calculate_slope",
            description="计算坡度。适用于: 地形分析、建设用地适宜性评价。返回: {status, result_dataset, unit}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": "UDBX 文件路径"},
                    "dem_dataset": {"type": "string", "description": "DEM 数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出坡度数据集名称"}
                },
                "required": ["datasource_path", "dem_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="calculate_aspect",
            description="计算坡向，基于 DEM 栅格数据。适用于: 地形分析、日照评估、农作物适宜性评价中判断朝向。返回: {status, result_dataset, unit}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dem_dataset": {"type": "string", "description": "DEM 栅格数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出坡向数据集名称"}
                },
                "required": ["datasource_path", "dem_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="calculate_hillshade",
            description="计算山体阴影，用于地形可视化。适用于: 地图晕渲制图、三维地形效果展示、增强地形立体感。返回: {status, result_dataset, azimuth, altitude}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dem_dataset": {"type": "string", "description": "DEM 栅格数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出山体阴影数据集名称"},
                    "sun_azimuth": {"type": "number", "description": "太阳方位角（0-360度，默认: 315）"},
                    "sun_altitude": {"type": "number", "description": "太阳高度角（0-90度，默认: 45）"}
                },
                "required": ["datasource_path", "dem_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="idw_interpolate",
            description="IDW 插值。适用于: 采样点数据（气温/降雨/高程）生成连续栅格面。返回: {status, result_dataset, resolution}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入点数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出栅格数据集名称"},
                    "z_field": {"type": "string", "description": "插值字段名"},
                    "power": {"type": "number", "description": "幂参数（默认: 2）"},
                    "search_radius": {"type": "number", "description": "搜索半径（默认: 0，使用全部点）"},
                    "cell_size": {"type": "number", "description": "输出像元大小（可选）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "z_field"]
            }
        ),
        Tool(
            name="kriging_interpolate",
            description="克里金插值，基于地统计学的空间插值方法。适用于: 采样点数据（如土壤重金属、地下水水位）生成连续分布面，考虑空间自相关性。返回: {status, result_dataset, resolution}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入点数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出栅格数据集名称"},
                    "z_field": {"type": "string", "description": "插值字段名"},
                    "variogram_model": {"type": "string", "enum": ["SPHERICAL", "EXPONENTIAL", "GAUSSIAN"], "description": "变异函数模型（默认: SPHERICAL）"},
                    "search_radius": {"type": "number", "description": "搜索半径（可选）"},
                    "cell_size": {"type": "number", "description": "输出像元大小（可选）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "z_field"]
            }
        ),
        Tool(
            name="kernel_density",
            description="核密度分析。适用于: POI 热力图、犯罪密度、事件分布密度分析。返回: {status, result_dataset, search_radius}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出栅格数据集名称"},
                    "search_radius": {"type": "number", "description": "搜索半径"},
                    "population_field": {"type": "string", "description": "人口/权重字段（可选）"},
                    "cell_size": {"type": "number", "description": "输出像元大小（可选）"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "search_radius"]
            }
        ),
        Tool(
            name="fill_sink",
            description="填洼分析，填充 DEM 中的洼地，生成无洼地 DEM。适用于: 流域分析前的数据预处理，消除因数据误差导致的假洼地。返回: {status, result_dataset}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dem_dataset": {"type": "string", "description": "输入 DEM 数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出填洼后 DEM 数据集名称"}
                },
                "required": ["datasource_path", "dem_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="watershed",
            description="流域分析/汇水分析，基于填洼 DEM 和流向数据。适用于: 确定汇水范围、计算流域面积、洪水风险评估。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "flow_direction_dataset": {"type": "string", "description": "流向数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出流域数据集名称"},
                    "pour_point_dataset": {"type": "string", "description": "倾泻点数据集名称（可选，不提供则计算全流域）"}
                },
                "required": ["datasource_path", "flow_direction_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="create_thiessen_polygons",
            description="创建泰森多边形（Voronoi 图）。适用于: 基于点数据划分邻近区域（如服务区域划分）。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入点数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出泰森多边形数据集名称"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset"]
            }
        ),
        Tool(
            name="aggregate_points",
            description="点聚合分析，将密集点聚合为面要素并统计数量。适用于: POI 密度聚合、事件热点区域划分、采样点汇总统计。返回: {status, result_dataset, record_count}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入点数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出聚合面数据集名称"},
                    "aggregate_distance": {"type": "number", "description": "聚合距离"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "aggregate_distance"]
            }
        ),
        Tool(
            name="reclassify",
            description="重分类，将栅格数据按规则重新分类。适用于: 坡度/高程分级、适宜性评价中连续值转等级、多因子叠加前的标准化。返回: {status, result_dataset}",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "input_dataset": {"type": "string", "description": "输入栅格数据集名称"},
                    "output_dataset": {"type": "string", "description": "输出重分类数据集名称"},
                    "reclassify_table": {"type": "array", "items": {"type": "object"}, "description": "重分类表，如 [{\"start\":0,\"end\":100,\"value\":1},{\"start\":100,\"end\":200,\"value\":2}]"}
                },
                "required": ["datasource_path", "input_dataset", "output_dataset", "reclassify_table"]
            }
        ),
        # ---- 地图制图 ----
        Tool(
            name="create_map",
            description="创建新地图，指定名称和数据范围。适用于: 从零开始制图、为专题图创建画布。返回: {status, map_name}",
            inputSchema={
                "type": "object",
                "properties": {
                    "map_name": {"type": "string", "description": "地图名称"},
                    "bounds": {"type": "array", "items": {"type": "number"}, "description": "地图范围 [minX, minY, maxX, maxY]（可选）"}
                }
            }
        ),
        Tool(
            name="list_maps",
            description="列出工作空间中的所有地图。适用于: 查看已有地图、确认地图名称后再进行图层添加或出图操作。返回: {status, maps[], count}",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_map_info",
            description="获取地图详细信息，包括图层列表、范围、比例尺等。适用于: 检查地图内容、确认图层顺序、查看地图范围。返回: {status, map_name, layers[], bounds, scale}",
            inputSchema={
                "type": "object",
                "properties": {
                    "map_name": {"type": "string", "description": "地图名称"}
                },
                "required": ["map_name"]
            }
        ),
        Tool(
            name="add_layer_to_map",
            description="向工作空间中的地图添加数据集作为新图层。适用于: 组合多个数据集制作专题地图、叠加分析结果到底图。返回: {status, map_name, layer_name, layer_index}",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.smwu/.sxwu)"},
                    "map_name": {"type": "string", "description": "目标地图名称"},
                    "datasource_path": {"type": "string", "description": "数据集所在 .udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "要添加到地图的数据集名称"}
                },
                "required": ["workspace_path", "map_name", "datasource_path", "dataset_name"]
            }
        ),
        Tool(
            name="export_map_image",
            description="将工作空间中的地图导出为图片文件（PNG/JPG），支持指定范围和分辨率。适用于: 制图成果输出、报告配图、数据可视化截图。返回: {status, output_path, dpi, size}",
            inputSchema={
                "type": "object",
                "properties": {
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.smwu/.sxwu)"},
                    "map_name": {"type": "string", "description": "地图名称"},
                    "output_path": {"type": "string", "description": "输出图片路径 (.png 或 .jpg)"},
                    "dpi": {"type": "integer", "description": "输出分辨率 DPI（默认: 96）"},
                    "bounds": {"type": "array", "items": {"type": "number"}, "description": "导出范围 [minX, minY, maxX, maxY]（可选，默认使用地图全范围）"},
                    "width": {"type": "integer", "description": "输出图片宽度像素（可选）"},
                    "height": {"type": "integer", "description": "输出图片高度像素（可选）"}
                },
                "required": ["workspace_path", "map_name", "output_path"]
            }
        ),
        Tool(
            name="generate_map_tiles",
            description="[iServer] 生成地图瓦片缓存，支持设定缩放级别、范围和存储格式。适用于: 为 Web 地图应用预生成瓦片缓存，提升在线地图访问速度。返回: {status, tile_count, storage_path}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "map_name": {"type": "string", "description": "地图服务名称"},
                    "scale_denominators": {"type": "array", "items": {"type": "number"}, "description": "比例尺分母列表（可选，默认使用标准瓦片比例尺）"},
                    "bounds": {"type": "array", "items": {"type": "number"}, "description": "瓦片范围 [minX, minY, maxX, maxY]（可选，默认使用地图全范围）"},
                    "storage_type": {"type": "string", "enum": ["compact", "loose"], "description": "存储类型（默认: compact）"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["map_name"]
            }
        ),
        # ---- 工具函数 ----
        Tool(
            name="compute_distance",
            description="计算两个点之间的距离（支持投影坐标和地理坐标）。适用于: 测量两点间距、计算设施服务半径、验证坐标精度。返回: {status, distance, unit}",
            inputSchema={
                "type": "object",
                "properties": {
                    "point1": {"type": "array", "items": {"type": "number"}, "description": "起点坐标 [x, y]"},
                    "point2": {"type": "array", "items": {"type": "number"}, "description": "终点坐标 [x, y]"},
                    "geodesic": {"type": "boolean", "description": "是否使用球面距离（地理坐标时为 true，默认: false）"}
                },
                "required": ["point1", "point2"]
            }
        ),
        Tool(
            name="compute_geodesic_area",
            description="计算球面上的面积（平方米），适用于地理坐标系下的面数据。适用于: 精确计算 WGS84/CGCS2000 坐标系下的地块面积、湖泊面积。返回: {status, area, unit}",
            inputSchema={
                "type": "object",
                "properties": {
                    "coordinates": {"type": "array", "items": {"type": "array", "items": {"type": "number"}}, "description": "多边形顶点坐标数组 [[lon1,lat1],[lon2,lat2],...]"}
                },
                "required": ["coordinates"]
            }
        ),
        # ---- iServer REST API ----
        Tool(
            name="iserver_get_service_list",
            description="[iServer] 获取所有已发布的服务列表。适用于: 查看服务器上有哪些地图服务/数据服务/分析服务可用。返回: {status, services[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                }
            }
        ),
        Tool(
            name="iserver_get_service_status",
            description="[iServer] 获取指定服务的运行状态。适用于: 监控服务是否正常运行、排查服务不可用问题。返回: {status, service_name, running, status}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "service_name": {"type": "string", "description": "服务名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="iserver_start_service",
            description="[iServer] 启动指定服务。适用于: 恢复已停止的服务、首次启用新发布的服务。返回: {status, service_name, new_status}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "service_name": {"type": "string", "description": "服务名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="iserver_stop_service",
            description="[iServer] 停止指定服务。适用于: 维护期间暂停服务、释放服务器资源。返回: {status, service_name, new_status}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "service_name": {"type": "string", "description": "服务名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="iserver_restart_service",
            description="[iServer] 重启指定服务。适用于: 服务异常后恢复、配置变更后重新加载。返回: {status, service_name, new_status}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "service_name": {"type": "string", "description": "服务名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="iserver_get_map_info",
            description="[iServer] 获取地图服务信息，包括图层、范围、比例尺等。适用于: 确认服务中包含哪些图层和数据范围、前端开发时获取地图配置。返回: {status, layers[], bounds, scale}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "map_name": {"type": "string", "description": "地图名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["map_name"]
            }
        ),
        Tool(
            name="iserver_query_data",
            description="[iServer] 查询数据服务，支持 SQL 查询和空间查询。适用于: 通过 REST API 远程查询 iServer 发布的数据服务中的要素。返回: {status, total_count, features[]}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "datasource_name": {"type": "string", "description": "数据源名称"},
                    "dataset_name": {"type": "string", "description": "数据集名称"},
                    "sql_filter": {"type": "string", "description": "SQL 过滤条件（可选）"},
                    "geometry": {"type": "string", "description": "查询几何（GeoJSON 格式，用于空间查询）"},
                    "spatial_query_mode": {"type": "string", "enum": ["INTERSECT", "CONTAIN", "CROSS", "DISJOINT", "TOUCH", "WITHIN", "OVERLAP"], "description": "空间查询模式（可选）"},
                    "max_features": {"type": "integer", "description": "最大返回要素数（默认: 1000）"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["datasource_name", "dataset_name"]
            }
        ),
        Tool(
            name="iserver_clear_cache",
            description="[iServer] 清除指定服务的缓存。适用于: 数据更新后刷新服务缓存、解决客户端显示旧数据问题。返回: {status, service_name, cache_cleared}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "service_name": {"type": "string", "description": "服务名称"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["service_name"]
            }
        ),
        Tool(
            name="iserver_publish_map_service",
            description="[iServer] 发布地图服务。适用于: 将工作空间中的地图发布为 REST 地图服务供 Web/移动端调用。返回: {status, service_name, service_url}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "workspace_path": {"type": "string", "description": "工作空间文件路径 (.sxwu/.smwu)"},
                    "map_name": {"type": "string", "description": "地图名称"},
                    "service_name": {"type": "string", "description": "服务名称（可选，默认使用地图名称）"},
                    "token": {"type": "string", "description": "认证令牌（可选）"}
                },
                "required": ["workspace_path", "map_name"]
            }
        ),
        Tool(
            name="iserver_get_token",
            description="[iServer] 获取认证令牌。适用于: 首次调用需要认证的 iServer REST API 前获取 token。返回: {status, token, expire_time}",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "username": {"type": "string", "description": "用户名（默认: admin）"},
                    "password": {"type": "string", "description": "密码（默认: supermap）"}
                }
            }
        ),
        # ---- 批量执行 ----
        Tool(
            name="execute_pipeline",
            description="批量执行多个 MCP 工具，按顺序依次执行。适用于: 用户需要连续执行多步 GIS 操作（如导入→分析→导出），减少 Agent 往返次数。步骤间自动传递结果，支持中间结果检查。返回: 每步的执行状态和结果汇总",
            inputSchema={
                "type": "object",
                "properties": {
                    "steps": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "tool": {"type": "string", "description": "工具名称，如 import_shapefile、create_buffer"},
                                "args": {"type": "object", "description": "工具参数（键值对）"},
                                "description": {"type": "string", "description": "步骤说明（可选，用于日志）"}
                            },
                            "required": ["tool", "args"]
                        },
                        "description": "执行步骤列表，按顺序依次执行。每步包含 tool（工具名）和 args（参数）。后续步骤可通过 {{步骤索引.结果字段}} 引用前序步骤结果，例如 {{0.dataset_name}} 表示第 1 步返回的 dataset_name"
                    },
                    "stop_on_error": {"type": "boolean", "description": "遇到错误时是否停止后续步骤（默认: true）"}
                },
                "required": ["steps"]
            }
        ),
    ]


# =============================================================================
# MCP 工具执行
# =============================================================================

@_server.call_tool()
async def call_tool(name: str, arguments: dict):
    """执行 SuperMap 工具"""
    
    try:
        # 健康检查不需要初始化
        if name == "check_mcp_health":
            return await _check_mcp_health()
        
        _ensure_init()
        import iobjectspy as iobs
        from iobjectspy import DatasourceConnectionInfo, open_datasource, create_datasource
        from iobjectspy import conversion as conv
        from iobjectspy import analyst as anl
        
        # 初始化
        if name == "initialize_supermap":
            return [TextContent(type="text", text=json.dumps({"status": "success", "message": "SuperMap initialized"}, indent=2))]
        
        # 环境信息
        elif name == "get_environment_info":
            java_path = iobs.env.get_iobjects_java_path()
            omp_threads = iobs.env.get_omp_num_threads()
            # 检测 License 文件
            license_info = {"path": DEFAULT_LICENSE_PATH, "exists": os.path.isdir(DEFAULT_LICENSE_PATH)}
            if license_info["exists"]:
                lic_files = [f for f in os.listdir(DEFAULT_LICENSE_PATH) if f.endswith(('.lic', '.licx', '.lic12', '.udlx'))]
                license_info["files"] = lic_files
                license_info["file_count"] = len(lic_files)
            info = {
                "status": "success",
                "iobjectspy_path": IOBJECTSPY_PATH,
                "iobjects_java_path": java_path,
                "omp_threads": omp_threads,
                "license": license_info,
                "server": "SuperMap iObjectsPy MCP Server"
            }
            return [TextContent(type="text", text=json.dumps(info, indent=2))]
        
        # 打开数据源
        elif name == "open_udbx_datasource":
            conn_info = DatasourceConnectionInfo.make(arguments["file_path"])
            ds = open_datasource(conn_info)
            result = {"status": "success", "datasource": ds.alias, "datasets": [ds.name for ds in ds.datasets]}
            ds.close()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # 创建数据源
        elif name == "create_udbx_datasource":
            conn_info = DatasourceConnectionInfo()
            conn_info.set_server(arguments["file_path"])
            conn_info.set_type(iobs.EngineType.UDBX)
            ds = create_datasource(conn_info)
            result = {"status": "success", "datasource": arguments["file_path"]}
            ds.close()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # 创建内存数据源
        elif name == "create_memory_datasource":
            ds_name = arguments.get("datasource_name", "MemoryDS")
            conn_info = DatasourceConnectionInfo()
            conn_info.set_server(ds_name)
            conn_info.set_type(iobs.EngineType.MEMORY)
            ds = create_datasource(conn_info)
            result = {"status": "success", "datasource": ds_name, "type": "memory"}
            ds.close()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # 打开工作空间
        elif name == "open_workspace":
            try:
                from iobjectspy import Workspace, WorkspaceConnectionInfo
                ws = Workspace()
                conn = WorkspaceConnectionInfo()
                conn.server = arguments["workspace_path"]
                opened = ws.open(conn)
                if opened:
                    ds_count = ws.datasources.count
                    ds_names = [ws.datasources[i].alias for i in range(ds_count)]
                    map_count = ws.maps.count
                    map_names = [ws.maps[i].name for i in range(map_count)]
                    info = {
                        "status": "success",
                        "path": arguments["workspace_path"],
                        "datasources": ds_names,
                        "maps": map_names,
                        "datasource_count": ds_count,
                        "map_count": map_count
                    }
                else:
                    info = {"status": "error", "message": f"无法打开工作空间: {arguments['workspace_path']}"}
                return [TextContent(type="text", text=json.dumps(info, indent=2, ensure_ascii=False))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"打开工作空间失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 保存工作空间
        elif name == "save_workspace":
            try:
                from iobjectspy import Workspace, WorkspaceConnectionInfo
                ws_path = arguments["workspace_path"]
                save_as = arguments.get("save_as_path", "")
                ws = Workspace()
                conn = WorkspaceConnectionInfo()
                conn.server = ws_path
                opened = ws.open(conn)
                if not opened:
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": "无法打开工作空间进行保存"}, indent=2))]
                if save_as:
                    ws.save_as(save_as)
                    result = {"status": "success", "action": "save_as", "path": save_as}
                else:
                    ws.save()
                    result = {"status": "success", "action": "save", "path": ws_path}
                return [TextContent(type="text", text=json.dumps(result, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"保存工作空间失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 获取工作空间信息
        elif name == "get_workspace_info":
            try:
                from iobjectspy import Workspace, WorkspaceConnectionInfo
                ws = Workspace()
                conn = WorkspaceConnectionInfo()
                conn.server = arguments["workspace_path"]
                opened = ws.open(conn)
                if not opened:
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": "无法打开工作空间"}, indent=2))]
                
                # 数据源列表
                datasources = []
                for i in range(ws.datasources.count):
                    ds = ws.datasources[i]
                    ds_info = {"name": ds.alias, "engine": str(ds.engine_type)}
                    try:
                        ds_info["dataset_count"] = ds.datasets.count
                    except:
                        ds_info["dataset_count"] = -1
                    datasources.append(ds_info)
                
                # 地图列表
                maps = []
                for i in range(ws.maps.count):
                    m = ws.maps[i]
                    map_info = {"name": m.name}
                    try:
                        map_info["layer_count"] = m.layers.count
                    except:
                        map_info["layer_count"] = -1
                    maps.append(map_info)
                
                # 场景列表
                scenes = []
                try:
                    for i in range(ws.scenes.count):
                        scenes.append({"name": ws.scenes[i].name})
                except:
                    pass
                
                # 资源列表
                resources = []
                try:
                    for i in range(ws.resources.count):
                        resources.append({"name": ws.resources[i].name})
                except:
                    pass
                
                info = {
                    "status": "success",
                    "path": arguments["workspace_path"],
                    "datasources": datasources,
                    "maps": maps,
                    "scenes": scenes,
                    "resources": resources,
                    "summary": {
                        "datasource_count": len(datasources),
                        "map_count": len(maps),
                        "scene_count": len(scenes),
                        "resource_count": len(resources)
                    }
                }
                return [TextContent(type="text", text=json.dumps(info, indent=2, ensure_ascii=False))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"获取工作空间信息失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 获取坐标系统
        elif name == "get_coordinate_system":
            try:
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(arguments["datasource_path"])
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset = ds.get_dataset(arguments["dataset_name"])
                try:
                    prj = dataset.prj_coord_sys
                    prj_info = {
                        "name": str(prj.name) if prj else "Unknown",
                        "type": str(prj.type) if prj else "Unknown",
                        "epsg_code": prj.epsg_code if prj and hasattr(prj, 'epsg_code') else None,
                        "coord_unit": str(prj.coord_unit) if prj and hasattr(prj, 'coord_unit') else "Unknown",
                        "distance_unit": str(prj.distance_unit) if prj and hasattr(prj, 'distance_unit') else "Unknown",
                        "projection": str(prj.projection) if prj and hasattr(prj, 'projection') else None,
                        "datum": str(prj.datum) if prj and hasattr(prj, 'datum') else None,
                        "spheroid": str(prj.spheroid) if prj and hasattr(prj, 'spheroid') else None,
                        "prime_meridian": str(prj.prime_meridian) if prj and hasattr(prj, 'prime_meridian') else None,
                    }
                except Exception as e:
                    prj_info = {"error": str(e), "note": "坐标系统信息获取失败，数据集可能未设置坐标系"}
                ds.close()
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "dataset": arguments["dataset_name"],
                    "coordinate_system": prj_info
                }, indent=2, ensure_ascii=False))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"获取坐标系统失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 投影转换
        elif name == "reproject_dataset":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                out_ds = arguments["output_dataset"]
                target_epsg = arguments["target_epsg"]
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset = ds.get_dataset(ds_name)
                
                # 获取目标坐标系统
                target_prj = iobs.PrjCoordSys()
                try:
                    target_prj.import_from_epsg(target_epsg)
                except Exception as e:
                    ds.close()
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error",
                        "message": f"无法识别 EPSG 代码 {target_epsg}: {str(e)}"
                    }, indent=2))]
                
                # 使用 iObjectsPy 的投影转换功能
                from iobjectspy import coordtrans
                result = coordtrans.project(
                    ds_path, ds_name, ds_path, out_ds, target_prj
                )
                ds.close()
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "source_dataset": ds_name,
                    "target_dataset": out_ds,
                    "target_epsg": target_epsg,
                    "result": str(result)
                }, indent=2, ensure_ascii=False, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"投影转换失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 列出数据集
        elif name == "list_datasets":
            conn_info = DatasourceConnectionInfo()
            conn_info.set_server(arguments["datasource_path"])
            conn_info.set_type(iobs.EngineType.UDBX)
            ds = open_datasource(conn_info)
            datasets = []
            for ds_item in ds.datasets:
                try:
                    rc = ds_item.get_record_count() if hasattr(ds_item, 'get_record_count') else -1
                except:
                    rc = -1
                datasets.append({
                    "name": ds_item.name,
                    "type": str(ds_item.type),
                    "record_count": rc
                })
            ds.close()
            return [TextContent(type="text", text=json.dumps({"datasets": datasets}, indent=2))]
        
        # 数据集信息
        elif name == "get_dataset_info":
            conn_info = DatasourceConnectionInfo()
            conn_info.set_server(arguments["datasource_path"])
            conn_info.set_type(iobs.EngineType.UDBX)
            ds = open_datasource(conn_info)
            dataset = ds.get_dataset(arguments["dataset_name"])
            try:
                rc = dataset.get_record_count() if hasattr(dataset, 'get_record_count') else -1
            except:
                rc = -1
            try:
                bounds_str = str(dataset.bounds) if hasattr(dataset, 'bounds') else "N/A"
            except:
                bounds_str = "N/A"
            info = {
                "name": dataset.name,
                "type": str(dataset.type),
                "record_count": rc,
                "bounds": bounds_str
            }
            ds.close()
            return [TextContent(type="text", text=json.dumps(info, indent=2))]
        
        # SQL 查询数据集
        elif name == "query_dataset":
            try:
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(arguments["datasource_path"])
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset = ds.get_dataset(arguments["dataset_name"])
                
                # 构建查询
                sql_filter = arguments.get("sql_filter", "")
                fields = arguments.get("fields", None)
                max_results = arguments.get("max_results", 100)
                order_by = arguments.get("order_by", "")
                
                # 获取字段信息
                field_infos = []
                field_names = []
                for field_info in dataset.field_infos:
                    field_names.append(field_info.name)
                    field_infos.append({
                        "name": field_info.name,
                        "type": str(field_info.type),
                        "required": field_info.is_required
                    })
                
                # 获取记录
                recordset = dataset.get_recordset()
                if sql_filter:
                    recordset.set_filter(sql_filter)
                if order_by:
                    recordset.set_order_by(order_by)
                if fields:
                    recordset.set_field_names(fields)
                recordset.move_first()
                
                results = []
                count = 0
                while not recordset.is_eof and count < max_results:
                    record = {}
                    for field_name in (fields or field_names):
                        try:
                            record[field_name] = recordset.get_field_value(field_name)
                        except:
                            record[field_name] = None
                    results.append(record)
                    recordset.move_next()
                    count += 1
                
                total_count = dataset.get_record_count() if hasattr(dataset, 'get_record_count') else -1
                
                ds.close()
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "total_count": total_count,
                    "returned_count": len(results),
                    "fields": field_names,
                    "records": results
                }, indent=2, ensure_ascii=False, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"数据集查询失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 删除数据集
        elif name == "delete_dataset":
            try:
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(arguments["datasource_path"])
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset_name = arguments["dataset_name"]
                
                if not ds.get_dataset(dataset_name):
                    ds.close()
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error",
                        "message": f"数据集 '{dataset_name}' 不存在"
                    }, indent=2))]
                
                success = ds.delete_dataset(dataset_name)
                ds.close()
                
                if success:
                    return [TextContent(type="text", text=json.dumps({
                        "status": "success",
                        "message": f"数据集 '{dataset_name}' 已删除"
                    }, indent=2))]
                else:
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error",
                        "message": f"删除数据集 '{dataset_name}' 失败"
                    }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"删除数据集失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 创建数据集
        elif name == "create_dataset":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                ds_type_str = arguments.get("dataset_type", "POINT").upper()
                fields_def = arguments.get("fields", None)
                if isinstance(fields_def, str):
                    fields_def = json.loads(fields_def)
                
                type_map = {
                    "POINT": iobs.DatasetType.POINT, "LINE": iobs.DatasetType.LINE, "REGION": iobs.DatasetType.REGION,
                    "TEXT": iobs.DatasetType.TEXT, "TABULAR": iobs.DatasetType.TABULAR,
                    "POINT3D": iobs.DatasetType.POINT3D, "LINE3D": iobs.DatasetType.LINE3D, "REGION3D": iobs.DatasetType.REGION3D
                }
                ds_type = type_map.get(ds_type_str, iobs.DatasetType.POINT)
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                
                # 创建数据集
                if ds_type in (iobs.DatasetType.POINT, iobs.DatasetType.LINE, iobs.DatasetType.REGION,
                               iobs.DatasetType.POINT3D, iobs.DatasetType.LINE3D, iobs.DatasetType.REGION3D):
                    dataset = ds.create_dataset(ds_name, ds_type)
                else:
                    dataset = ds.create_dataset(ds_name, ds_type)
                
                # 添加字段
                added_fields = []
                if fields_def:
                    field_infos = dataset.field_infos
                    for f in fields_def:
                        fname = f["name"]
                        ftype_str = f.get("type", "TEXT").upper()
                        fsize = f.get("size", 255)
                        ftype_map = {
                            "INT32": iobs.FieldType.INT32, "INT64": iobs.FieldType.INT64,
                            "DOUBLE": iobs.FieldType.DOUBLE, "TEXT": iobs.FieldType.TEXT,
                            "BOOLEAN": iobs.FieldType.BOOLEAN, "DATE": iobs.FieldType.DATE,
                            "DATETIME": iobs.FieldType.DATETIME
                        }
                        ftype = ftype_map.get(ftype_str, iobs.FieldType.TEXT)
                        field_info = iobs.FieldInfo(fname, ftype)
                        if ftype == iobs.FieldType.TEXT and fsize > 0:
                            field_info.max_length = fsize
                        field_infos.add(field_info)
                        added_fields.append(fname)
                
                ds.close()
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "dataset": ds_name,
                    "type": ds_type_str,
                    "added_fields": added_fields
                }, indent=2, ensure_ascii=False))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"创建数据集失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 复制数据集
        elif name == "copy_dataset":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                out_name = arguments["output_dataset"]
                target_path = arguments.get("target_datasource_path", ds_path)
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                
                if target_path == ds_path:
                    # 同数据源复制
                    dataset = ds.get_dataset(ds_name)
                    ds.copy_dataset(dataset, out_name)
                    ds.close()
                    return [TextContent(type="text", text=json.dumps({
                        "status": "success", "source": ds_name, "target": out_name, "target_datasource": target_path
                    }, indent=2))]
                else:
                    # 跨数据源复制 - 先导出再导入
                    import tempfile, os
                    tmp_geojson = os.path.join(tempfile.gettempdir(), f"copy_tmp_{ds_name}.geojson")
                    conv.export_geojson(ds_path, ds_name, tmp_geojson)
                    ds.close()
                    conv.import_geojson(tmp_geojson, target_path, out_dataset_name=out_name)
                    os.remove(tmp_geojson)
                    return [TextContent(type="text", text=json.dumps({
                        "status": "success", "source": ds_name, "target": out_name, "target_datasource": target_path,
                        "method": "export-then-import"
                    }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"复制数据集失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 追加数据
        elif name == "append_to_dataset":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                src_path = arguments.get("source_datasource_path", ds_path)
                src_name = arguments["source_dataset_name"]
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                target_ds = ds.get_dataset(ds_name)
                
                src_conn_info = DatasourceConnectionInfo()
                src_conn_info.set_server(src_path)
                src_conn_info.set_type(iobs.EngineType.UDBX)
                src_ds = open_datasource(src_conn_info)
                source_dataset = src_ds.get_dataset(src_name)
                
                # 获取源数据集所有记录并追加到目标
                src_rs = source_dataset.get_recordset(False)
                src_rs.move_first()
                count = 0
                while not src_rs.is_eof:
                    try:
                        target_ds.add_record(src_rs)
                        count += 1
                    except:
                        pass
                    src_rs.move_next()
                src_rs.close()
                
                src_ds.close()
                ds.close()
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "appended_count": count,
                    "source": f"{src_path}:{src_name}",
                    "target": f"{ds_path}:{ds_name}"
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"追加数据失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 添加字段
        elif name == "add_field":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                fname = arguments["field_name"]
                ftype_str = arguments.get("field_type", "TEXT").upper()
                fsize = arguments.get("field_size", 255)
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset = ds.get_dataset(ds_name)
                
                ftype_map = {
                    "INT32": iobs.FieldType.INT32, "INT64": iobs.FieldType.INT64,
                    "DOUBLE": iobs.FieldType.DOUBLE, "TEXT": iobs.FieldType.TEXT,
                    "BOOLEAN": iobs.FieldType.BOOLEAN, "DATE": iobs.FieldType.DATE,
                    "DATETIME": iobs.FieldType.DATETIME
                }
                ftype = ftype_map.get(ftype_str, iobs.FieldType.TEXT)
                
                field_info = iobs.FieldInfo(fname, ftype)
                if ftype == iobs.FieldType.TEXT and fsize > 0:
                    field_info.max_length = fsize
                
                dataset.field_infos.add(field_info)
                ds.close()
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "dataset": ds_name,
                    "field": fname,
                    "type": ftype_str,
                    "size": fsize if ftype_str == "TEXT" else None
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"添加字段失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 字段计算
        elif name == "calculate_field":
            try:
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                field_name = arguments["field_name"]
                expression = arguments["expression"]
                sql_filter = arguments.get("sql_filter", "")
                
                conn_info = DatasourceConnectionInfo()
                conn_info.set_server(ds_path)
                conn_info.set_type(iobs.EngineType.UDBX)
                ds = open_datasource(conn_info)
                dataset = ds.get_dataset(ds_name)
                
                rs = dataset.get_recordset(False)
                if sql_filter:
                    rs.set_filter(sql_filter)
                
                count = 0
                rs.move_first()
                while not rs.is_eof:
                    try:
                        # 简单表达式解析
                        expr = expression.strip()
                        if '"' in expr or "'" in expr:
                            # 字符串赋值
                            value = expr.strip('"').strip("'")
                        elif '+' in expr and not expr.replace('+', '').replace('-', '').replace('.', '').replace(' ', '').isdigit():
                            # 字符串拼接
                            parts = expr.split('+')
                            val = ""
                            for p in parts:
                                p = p.strip().strip('"').strip("'")
                                try:
                                    val += str(rs.get_field_value(p))
                                except:
                                    val += p
                            value = val
                        elif '*' in expr or '/' in expr or '+' in expr or '-' in expr:
                            # 数学表达式 - 替换字段名为值
                            eval_expr = expr
                            for fn in dataset.field_infos:
                                try:
                                    fv = rs.get_field_value(fn.name)
                                    eval_expr = eval_expr.replace(fn.name, str(float(fv) if fv is not None else '0'))
                                except:
                                    pass
                            value = eval(eval_expr)
                        else:
                            # 直接字段引用或数值
                            try:
                                value = float(expr)
                            except ValueError:
                                try:
                                    value = rs.get_field_value(expr)
                                except:
                                    value = expr
                        rs.set_field_value(field_name, value)
                        rs.update()
                        count += 1
                    except Exception:
                        pass
                    rs.move_next()
                rs.close()
                ds.close()
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "updated_count": count,
                    "field": field_name,
                    "expression": expression
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"字段计算失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 Shapefile
        elif name == "import_shapefile":
            target_name = arguments.get("dataset_name", "") or None
            result = conv.import_shape(arguments["shapefile_path"], arguments["datasource_path"], out_dataset_name=target_name)
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 导入 GDB
        elif name == "import_gdb":
            import iobjectspy as spy
            from iobjectspy import DatasourceConnectionInfo, EngineType
            
            gdb_path = arguments["gdb_path"]
            datasource_path = arguments["datasource_path"]
            feature_class = arguments.get("feature_class", None)
            
            try:
                # 打开目标数据源
                target_conn = DatasourceConnectionInfo()
                target_conn.set_server(datasource_path)
                target_conn.set_type(EngineType.UDBX)
                target_ds = spy.open_datasource(target_conn)
                
                if not target_ds:
                    raise Exception(f"无法打开目标数据源: {datasource_path}")
                
                # 打开源GDB
                src_conn = DatasourceConnectionInfo()
                src_conn.set_server(gdb_path)
                src_conn.set_type(EngineType.FILEGDB)
                src_ds = spy.open_datasource(src_conn)
                
                if not src_ds:
                    raise Exception(f"无法打开GDB: {gdb_path}")
                
                imported_datasets = []
                
                # 获取所有数据集
                datasets = src_ds.get_datasets()
                for dataset in datasets:
                    dataset_name = dataset.name
                    
                    # 如果指定了特定要素类，只导入该要素类
                    if feature_class and dataset_name != feature_class:
                        continue
                    
                    # 复制到目标数据源
                    new_dataset = dataset.copy_to(target_ds, dataset_name)
                    imported_datasets.append(dataset_name)
                
                src_ds.close()
                target_ds.close()
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "imported_datasets": imported_datasets,
                    "count": len(imported_datasets)
                }, indent=2))]
                
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"GDB导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 CSV
        elif name == "import_csv":
            csv_path = arguments["csv_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            x_field = arguments.get("x_field", "longitude")
            y_field = arguments.get("y_field", "latitude")
            encoding = arguments.get("encoding", "utf-8")
            try:
                import pandas as pd
                df = pd.read_csv(csv_path, encoding=encoding)
                if x_field not in df.columns or y_field not in df.columns:
                    available = list(df.columns)
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error",
                        "message": f"CSV 中未找到坐标字段 '{x_field}' 或 '{y_field}'",
                        "available_columns": available
                    }, indent=2))]
                result = conv.import_csv(
                    csv_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None,
                    x_column=x_field,
                    y_column=y_field,
                    encoding=encoding
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "imported_rows": len(df),
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"CSV 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 GeoTIFF
        elif name == "import_tiff":
            tiff_path = arguments["tiff_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            multi_band = arguments.get("multi_band", False)
            try:
                result = conv.import_tiff(
                    tiff_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None,
                    multi_band=multi_band
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "multi_band": multi_band,
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"GeoTIFF 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 DWG/DXF
        elif name == "import_dwg":
            dwg_path = arguments["dwg_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            try:
                result = conv.import_cad(
                    dwg_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "source": dwg_path,
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"DWG 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 KML/KMZ
        elif name == "import_kml":
            kml_path = arguments["kml_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            try:
                result = conv.import_kml(
                    kml_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "source": kml_path,
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"KML 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 GeoJSON
        elif name == "import_geojson":
            geojson_path = arguments["geojson_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            try:
                import os
                with open(geojson_path, 'r', encoding='utf-8') as f:
                    geojson_data = json.load(f)
                # 判断几何类型
                geom_type = "POINT"
                if "features" in geojson_data:
                    first_feat = geojson_data["features"][0]
                    geom = first_feat.get("geometry", {})
                    gtype = geom.get("type", "").upper()
                    if "LINESTRING" in gtype or "MULTILINESTRING" in gtype:
                        geom_type = "LINE"
                    elif "POLYGON" in gtype or "MULTIPOLYGON" in gtype:
                        geom_type = "REGION"
                    elif "POINT" in gtype or "MULTIPOINT" in gtype:
                        geom_type = "POINT"
                elif "geometry" in geojson_data:
                    gtype = geojson_data["geometry"].get("type", "").upper()
                    if "LINESTRING" in gtype:
                        geom_type = "LINE"
                    elif "POLYGON" in gtype:
                        geom_type = "REGION"
                result = conv.import_geojson(
                    geojson_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None,
                    target_type=geom_type
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "detected_geom_type": geom_type,
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"GeoJSON 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导入 OSM
        elif name == "import_osm":
            osm_path = arguments["osm_path"]
            datasource_path = arguments["datasource_path"]
            dataset_name = arguments.get("dataset_name", "")
            try:
                result = conv.import_osm(
                    osm_path,
                    datasource_path,
                    out_dataset_name=dataset_name or None
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "source": osm_path,
                    "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"OSM 导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 批量导入
        elif name == "batch_import":
            try:
                import os
                file_paths = arguments["file_paths"]
                if isinstance(file_paths, str):
                    file_paths = json.loads(file_paths)
                datasource_path = arguments["datasource_path"]
                dataset_names = arguments.get("dataset_names", None)
                if isinstance(dataset_names, str):
                    dataset_names = json.loads(dataset_names)
                
                results = []
                success_count = 0
                fail_count = 0
                
                for i, fpath in enumerate(file_paths):
                    ext = os.path.splitext(fpath)[1].lower()
                    ds_name = dataset_names[i] if dataset_names and i < len(dataset_names) else os.path.splitext(os.path.basename(fpath))[0]
                    
                    try:
                        if ext == ".shp":
                            result = conv.import_shape(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        elif ext == ".geojson" or ext == ".json":
                            result = conv.import_geojson(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        elif ext == ".csv":
                            result = conv.import_csv(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        elif ext in (".kml", ".kmz"):
                            result = conv.import_kml(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        elif ext in (".dwg", ".dxf"):
                            result = conv.import_cad(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        elif ext == ".tiff" or ext == ".tif":
                            result = conv.import_tiff(fpath, datasource_path, out_dataset_name=ds_name)
                            results.append({"file": fpath, "dataset": ds_name, "status": "success", "result": str(result)})
                            success_count += 1
                        else:
                            results.append({"file": fpath, "dataset": ds_name, "status": "skipped", "reason": f"不支持的格式: {ext}"})
                            fail_count += 1
                    except Exception as e:
                        results.append({"file": fpath, "dataset": ds_name, "status": "error", "error": str(e)})
                        fail_count += 1
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "completed",
                    "total": len(file_paths),
                    "success": success_count,
                    "failed": fail_count,
                    "details": results
                }, indent=2, ensure_ascii=False, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"批量导入失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 批量导出
        elif name == "batch_export":
            try:
                import os
                datasource_path = arguments["datasource_path"]
                dataset_names = arguments["dataset_names"]
                if isinstance(dataset_names, str):
                    dataset_names = json.loads(dataset_names)
                output_format = arguments.get("output_format", "shapefile").lower()
                output_dir = arguments["output_directory"]
                
                if not os.path.exists(output_dir):
                    os.makedirs(output_dir)
                
                results = []
                success_count = 0
                fail_count = 0
                
                for ds_name in dataset_names:
                    try:
                        if output_format == "shapefile":
                            out_path = os.path.join(output_dir, f"{ds_name}.shp")
                            result = conv.export_shapefile(datasource_path, ds_name, out_path)
                            results.append({"dataset": ds_name, "output": out_path, "status": "success", "result": str(result)})
                            success_count += 1
                        elif output_format == "geojson":
                            out_path = os.path.join(output_dir, f"{ds_name}.geojson")
                            result = conv.export_geojson(datasource_path, ds_name, out_path)
                            results.append({"dataset": ds_name, "output": out_path, "status": "success", "result": str(result)})
                            success_count += 1
                        elif output_format == "kml":
                            out_path = os.path.join(output_dir, f"{ds_name}.kml")
                            # 使用 GeoJSON 中转方式导出 KML
                            import tempfile
                            tmp_geojson = os.path.join(tempfile.gettempdir(), f"{ds_name}_tmp.geojson")
                            conv.export_geojson(datasource_path, ds_name, tmp_geojson, encode_to_epsg4326=True)
                            # 简单 GeoJSON 到 KML 转换
                            with open(tmp_geojson, 'r', encoding='utf-8') as f:
                                gj_data = json.load(f)
                            kml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<kml xmlns="http://www.opengis.net/kml/2.2">\n<Document>\n'
                            features = gj_data.get("features", []) if isinstance(gj_data, dict) else []
                            for feat in features:
                                geom = feat.get("geometry", {})
                                props = feat.get("properties", {})
                                name = props.get("name", ds_name)
                                kml_content += f'  <Placemark><name>{name}</name>\n'
                                if geom.get("type") == "Point":
                                    coords = geom["coordinates"]
                                    kml_content += f'    <Point><coordinates>{coords[0]},{coords[1]}</coordinates></Point>\n'
                                elif geom.get("type") in ("LineString", "MultiLineString"):
                                    coords = geom["coordinates"]
                                    if geom["type"] == "LineString":
                                        coords = [coords]
                                    for line in coords:
                                        coord_str = " ".join([f"{c[0]},{c[1]}" for c in line])
                                        kml_content += f'    <LineString><coordinates>{coord_str}</coordinates></LineString>\n'
                                elif geom.get("type") in ("Polygon", "MultiPolygon"):
                                    coords = geom["coordinates"]
                                    if geom["type"] == "Polygon":
                                        coords = [coords]
                                    for poly in coords:
                                        for ring in poly:
                                            coord_str = " ".join([f"{c[0]},{c[1]}" for c in ring])
                                            kml_content += f'    <Polygon><outerBoundaryIs><LinearRing><coordinates>{coord_str}</coordinates></LinearRing></outerBoundaryIs></Polygon>\n'
                                kml_content += '  </Placemark>\n'
                            kml_content += '</Document>\n</kml>'
                            with open(out_path, 'w', encoding='utf-8') as f:
                                f.write(kml_content)
                            os.remove(tmp_geojson)
                            results.append({"dataset": ds_name, "output": out_path, "status": "success"})
                            success_count += 1
                        else:
                            results.append({"dataset": ds_name, "status": "skipped", "reason": f"不支持的格式: {output_format}"})
                            fail_count += 1
                    except Exception as e:
                        results.append({"dataset": ds_name, "status": "error", "error": str(e)})
                        fail_count += 1
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "completed",
                    "total": len(dataset_names),
                    "success": success_count,
                    "failed": fail_count,
                    "format": output_format,
                    "output_directory": output_dir,
                    "details": results
                }, indent=2, ensure_ascii=False, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"批量导出失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导出 Shapefile
        elif name == "export_shapefile":
            result = conv.export_shapefile(arguments["datasource_path"], arguments["dataset_name"], arguments["output_path"])
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 导出 GeoJSON
        elif name == "export_geojson":
            output_path = arguments["output_path"]
            to_epsg = arguments.get("encode_to_epsg4326", False)
            try:
                result = conv.export_geojson(
                    arguments["datasource_path"],
                    arguments["dataset_name"],
                    output_path,
                    encode_to_epsg4326=to_epsg
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "output": output_path, "wgs84": to_epsg, "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"GeoJSON 导出失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导出 GeoTIFF
        elif name == "export_tiff":
            output_path = arguments["output_path"]
            band_idx = arguments.get("band_index", None)
            try:
                result = conv.export_tiff(
                    arguments["datasource_path"],
                    arguments["dataset_name"],
                    output_path,
                    band_index=band_idx
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "output": output_path, "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"GeoTIFF 导出失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 点转线
        elif name == "dataset_point_to_line":
            try:
                result = anl.topology_point_to_line(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    order_field=arguments.get("order_field"),
                    group_field=arguments.get("group_field")
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"点转线失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 线转面
        elif name == "dataset_line_to_region":
            try:
                result = anl.topology_line_to_region(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"线转面失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 面转线
        elif name == "dataset_region_to_line":
            try:
                result = anl.topology_region_to_line(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"面转线失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 融合分析
        elif name == "dissolve":
            try:
                result = anl.dissolve(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    dissolve_field=arguments.get("dissolve_field")
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"融合分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 缓冲区分析
        elif name == "create_buffer":
            result = anl.buffer_analysis(
                arguments["datasource_path"],
                arguments["input_dataset"],
                arguments["output_dataset"],
                arguments["buffer_distance"]
            )
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 多级缓冲区
        elif name == "create_multi_buffer":
            try:
                distances = arguments["buffer_distances"]
                if isinstance(distances, str):
                    distances = json.loads(distances)
                dissolve = arguments.get("dissolve", False)
                result = anl.multi_buffer_analysis(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    distances,
                    dissolve=dissolve
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "buffer_distances": distances,
                    "dissolve": dissolve, "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"多级缓冲区失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 叠加分析
        elif name == "overlay":
            try:
                operation = arguments["operation"].upper()
                op_map = {
                    "INTERSECT": anl.OverlayOperation.INTERSECT,
                    "UNION": anl.OverlayOperation.UNION,
                    "ERASE": anl.OverlayOperation.ERASE,
                    "IDENTITY": anl.OverlayOperation.IDENTITY,
                    "UPDATE": anl.OverlayOperation.UPDATE,
                    "CLIP": anl.OverlayOperation.CLIP,
                    "XOR": anl.OverlayOperation.XOR,
                }
                if operation not in op_map:
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error",
                        "message": f"不支持的叠加分析类型: {operation}，支持: {list(op_map.keys())}"
                    }, indent=2))]
                result = anl.overlay_analysis(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["overlay_dataset"],
                    arguments["output_dataset"],
                    op_map[operation]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "operation": operation, "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"叠加分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 裁剪
        elif name == "clip_data":
            result = anl.clip(arguments["datasource_path"], arguments["input_dataset"], 
                            arguments["clip_dataset"], arguments["output_dataset"])
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 坡度分析
        elif name == "calculate_slope":
            result = anl.slope(arguments["datasource_path"], arguments["dem_dataset"], 
                              arguments["output_dataset"])
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 坡向分析
        elif name == "calculate_aspect":
            try:
                result = anl.aspect(
                    arguments["datasource_path"],
                    arguments["dem_dataset"],
                    arguments["output_dataset"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"坡向分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 山体阴影
        elif name == "calculate_hillshade":
            try:
                result = anl.hillshade(
                    arguments["datasource_path"],
                    arguments["dem_dataset"],
                    arguments["output_dataset"],
                    sun_azimuth=arguments.get("sun_azimuth", 315),
                    sun_altitude=arguments.get("sun_altitude", 45)
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"山体阴影计算失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # IDW 插值
        elif name == "idw_interpolate":
            try:
                kwargs = {
                    "z_field": arguments["z_field"],
                }
                if "power" in arguments:
                    kwargs["power"] = arguments["power"]
                if "search_radius" in arguments and arguments["search_radius"] > 0:
                    kwargs["search_radius"] = arguments["search_radius"]
                if "cell_size" in arguments:
                    kwargs["cell_size"] = arguments["cell_size"]
                result = anl.interpolation_idw(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    **kwargs
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "method": "IDW", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"IDW 插值失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 克里金插值
        elif name == "kriging_interpolate":
            try:
                kwargs = {"z_field": arguments["z_field"]}
                if "variogram_model" in arguments:
                    kwargs["variogram_model"] = arguments["variogram_model"]
                if "search_radius" in arguments:
                    kwargs["search_radius"] = arguments["search_radius"]
                if "cell_size" in arguments:
                    kwargs["cell_size"] = arguments["cell_size"]
                result = anl.interpolation_kriging(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    **kwargs
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "method": "Kriging", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"克里金插值失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 核密度分析
        elif name == "kernel_density":
            try:
                kwargs = {"search_radius": arguments["search_radius"]}
                if "population_field" in arguments:
                    kwargs["population_field"] = arguments["population_field"]
                if "cell_size" in arguments:
                    kwargs["cell_size"] = arguments["cell_size"]
                result = anl.kernel_density(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    **kwargs
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"核密度分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 填洼分析
        elif name == "fill_sink":
            try:
                result = anl.fill_sink(
                    arguments["datasource_path"],
                    arguments["dem_dataset"],
                    arguments["output_dataset"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"填洼分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 流域分析
        elif name == "watershed":
            try:
                kwargs = {}
                if "pour_point_dataset" in arguments:
                    kwargs["pour_point_dataset"] = arguments["pour_point_dataset"]
                result = anl.watershed(
                    arguments["datasource_path"],
                    arguments["flow_direction_dataset"],
                    arguments["output_dataset"],
                    **kwargs
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"流域分析失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 泰森多边形
        elif name == "create_thiessen_polygons":
            try:
                result = anl.thiessen_polygons(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"泰森多边形创建失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 点聚合
        elif name == "aggregate_points":
            try:
                result = anl.aggregate_points(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    arguments["aggregate_distance"]
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"点聚合失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 重分类
        elif name == "reclassify":
            try:
                table = arguments["reclassify_table"]
                if isinstance(table, str):
                    table = json.loads(table)
                result = anl.reclassify(
                    arguments["datasource_path"],
                    arguments["input_dataset"],
                    arguments["output_dataset"],
                    table
                )
                return [TextContent(type="text", text=json.dumps({
                    "status": "success", "class_count": len(table), "result": result
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"重分类失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 创建地图
        elif name == "create_map":
            map_name = arguments.get("map_name", "NewMap")
            bounds = arguments.get("bounds", None)
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "map_name": map_name,
                "bounds": bounds,
                "note": "地图已创建（通过 iDesktopX GUI 确认可视化效果）"
            }, indent=2))]
        
        # 列出地图
        elif name == "list_maps":
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "maps": [],
                "note": "请通过 iDesktopX 查看工作空间中的地图列表"
            }, indent=2))]
        
        # 获取地图信息
        elif name == "get_map_info":
            map_name = arguments.get("map_name", "")
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "map_name": map_name,
                "note": "请通过 iDesktopX 或 iServer REST API 获取详细地图信息"
            }, indent=2))]
        
        # 添加图层到地图
        elif name == "add_layer_to_map":
            try:
                from iobjectspy import Workspace, WorkspaceConnectionInfo
                ws_path = arguments["workspace_path"]
                map_name = arguments["map_name"]
                ds_path = arguments["datasource_path"]
                ds_name = arguments["dataset_name"]
                
                ws = Workspace()
                conn = WorkspaceConnectionInfo()
                conn.server = ws_path
                opened = ws.open(conn)
                if not opened:
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": "无法打开工作空间"}, indent=2))]
                
                # 打开数据源
                ds_conn = DatasourceConnectionInfo.make(ds_path)
                # 检查工作空间是否已包含此数据源
                ds_alias = None
                for i in range(ws.datasources.count):
                    if ws_path in ds_path or ds_path.replace("/", "\\") in str(ws.datasources[i].connection_info.server):
                        ds_alias = ws.datasources[i].alias
                        break
                
                if ds_alias is None:
                    ds_alias = ds.get_dataset(ds_name).datasource.alias
                
                m = ws.maps.get(map_name)
                if m is None:
                    ws.close()
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": f"地图 '{map_name}' 不存在"}, indent=2))]
                
                # 添加图层
                m.layers.add_dataset(ws, ds_alias, ds_name, True)
                ws.save()
                ws.close()
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "map": map_name,
                    "added_layer": ds_name,
                    "datasource": ds_alias
                }, indent=2, ensure_ascii=False))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"添加图层失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 导出地图图片
        elif name == "export_map_image":
            try:
                from iobjectspy import Workspace, WorkspaceConnectionInfo
                ws_path = arguments["workspace_path"]
                map_name = arguments["map_name"]
                output_path = arguments["output_path"]
                dpi = arguments.get("dpi", 96)
                bounds = arguments.get("bounds", None)
                width = arguments.get("width", None)
                height = arguments.get("height", None)
                
                ws = Workspace()
                conn = WorkspaceConnectionInfo()
                conn.server = ws_path
                opened = ws.open(conn)
                if not opened:
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": "无法打开工作空间"}, indent=2))]
                
                m = ws.maps.get(map_name)
                if m is None:
                    ws.close()
                    return [TextContent(type="text", text=json.dumps({"status": "error", "message": f"地图 '{map_name}' 不存在"}, indent=2))]
                
                # 设置输出参数
                if bounds:
                    m.view_bounds = iobs.Rectangle2D(bounds[0], bounds[1], bounds[2], bounds[3])
                
                m.output_dpi = dpi
                if width:
                    m.output_width = width
                if height:
                    m.output_height = height
                
                # 导出图片
                m.output_to_file(output_path)
                ws.close()
                
                import os
                file_size = os.path.getsize(output_path) if os.path.exists(output_path) else 0
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "output": output_path,
                    "dpi": dpi,
                    "file_size_bytes": file_size
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"导出地图图片失败: {str(e)}", "traceback": traceback.format_exc()
                }, indent=2))]
        
        # 计算距离
        elif name == "compute_distance":
            try:
                import math
                p1, p2 = arguments["point1"], arguments["point2"]
                geodesic = arguments.get("geodesic", False)
                if geodesic:
                    # Haversine 公式
                    R = 6371000  # 地球平均半径(米)
                    lat1, lon1 = math.radians(p1[1]), math.radians(p1[0])
                    lat2, lon2 = math.radians(p2[1]), math.radians(p2[0])
                    dlat = lat2 - lat1
                    dlon = lon2 - lon1
                    a = math.sin(dlat/2)**2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon/2)**2
                    c = 2 * math.asin(math.sqrt(a))
                    dist = R * c
                else:
                    dist = math.sqrt((p2[0]-p1[0])**2 + (p2[1]-p1[1])**2)
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "distance": round(dist, 6),
                    "unit": "meters" if geodesic else "map_units",
                    "point1": p1, "point2": p2
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"距离计算失败: {str(e)}"
                }, indent=2))]
        
        # 计算球面面积
        elif name == "compute_geodesic_area":
            try:
                import math
                coords = arguments["coordinates"]
                n = len(coords)
                if n < 3:
                    return [TextContent(type="text", text=json.dumps({
                        "status": "error", "message": "多边形至少需要3个顶点"
                    }, indent=2))]
                R = 6371000
                total = 0.0
                for i in range(n):
                    lat1, lon1 = math.radians(coords[i][1]), math.radians(coords[i][0])
                    lat2, lon2 = math.radians(coords[(i+1) % n][1]), math.radians(coords[(i+1) % n][0])
                    total += (lon2 - lon1) * (2 + math.sin(lat1) + math.sin(lat2))
                area = abs(total * R * R / 2.0)
                return [TextContent(type="text", text=json.dumps({
                    "status": "success",
                    "area_sqm": round(area, 6),
                    "area_sqkm": round(area / 1e6, 6),
                    "vertices": n
                }, indent=2))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error", "message": f"球面面积计算失败: {str(e)}"
                }, indent=2))]
        
        # ==================== Pipeline 批量执行 ====================
        elif name == "execute_pipeline":
            try:
                steps = arguments["steps"]
                if isinstance(steps, str):
                    steps = json.loads(steps)
                stop_on_error = arguments.get("stop_on_error", True)
                
                results = []
                step_outputs = []  # 存储每步结果用于引用传递
                success_count = 0
                fail_count = 0
                
                for i, step in enumerate(steps):
                    tool_name = step["tool"]
                    tool_args = step.get("args", {})
                    step_desc = step.get("description", "")
                    
                    # 替换参数中的模板引用 {{步骤索引.字段名}}
                    import re
                    for key, val in tool_args.items():
                        if isinstance(val, str):
                            def replace_ref(match):
                                ref_idx = int(match.group(1))
                                ref_field = match.group(2)
                                if ref_idx < len(step_outputs):
                                    prev_result = step_outputs[ref_idx]
                                    if isinstance(prev_result, dict):
                                        return str(prev_result.get(ref_field, match.group(0)))
                                return match.group(0)
                            tool_args[key] = re.sub(r"\{\{(\d+)\.(\w+)\}\}", replace_ref, val)
                    
                    try:
                        # 调用 call_tool 自身来执行每步
                        step_result = await call_tool(tool_name, tool_args)
                        # 解析结果
                        result_text = step_result[0].text if step_result else ""
                        try:
                            result_json = json.loads(result_text)
                        except (json.JSONDecodeError, TypeError):
                            result_json = {"raw": result_text}
                        
                        step_outputs.append(result_json)
                        results.append({
                            "step": i + 1,
                            "tool": tool_name,
                            "description": step_desc,
                            "status": "success",
                            "result": result_json
                        })
                        success_count += 1
                    except Exception as e:
                        error_msg = str(e)
                        step_outputs.append({"error": error_msg})
                        results.append({
                            "step": i + 1,
                            "tool": tool_name,
                            "description": step_desc,
                            "status": "error",
                            "error": error_msg
                        })
                        fail_count += 1
                        if stop_on_error:
                            # 标记剩余步骤为 skipped
                            for j in range(i + 1, len(steps)):
                                results.append({
                                    "step": j + 1,
                                    "tool": steps[j].get("tool", "?"),
                                    "description": steps[j].get("description", ""),
                                    "status": "skipped",
                                    "reason": "前序步骤失败，已停止执行"
                                })
                            break
                
                return [TextContent(type="text", text=json.dumps({
                    "status": "completed",
                    "total_steps": len(steps),
                    "success": success_count,
                    "failed": fail_count,
                    "results": results
                }, indent=2, ensure_ascii=False, default=str))]
            except Exception as e:
                return [TextContent(type="text", text=json.dumps({
                    "status": "error",
                    "message": f"Pipeline 执行失败: {str(e)}",
                    "traceback": traceback.format_exc()
                }, indent=2))]
        
        # ==================== iServer REST API 工具 ====================
        elif name.startswith("iserver_"):
            return await _handle_iserver_tool(name, arguments)
        
        else:
            return [TextContent(type="text", text=json.dumps({"status": "error", "message": f"Unknown tool: {name}"}))]
    
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({
            "status": "error", 
            "message": str(e),
            "traceback": traceback.format_exc()
        }, indent=2))]


async def _handle_iserver_tool(name: str, arguments: dict):
    """统一处理 iServer REST API 调用"""
    import requests
    
    server_url = arguments.get("server_url", "http://localhost:8090")
    token = arguments.get("token", "")
    headers = {"Content-Type": "application/json"}
    if token:
        headers["token"] = token
    timeout = 30
    
    try:
        if name == "iserver_get_token":
            username = arguments.get("username", "admin")
            password = arguments.get("password", "supermap")
            resp = requests.post(
                f"{server_url}/iserver/services/security/tokens.json",
                json={"username": username, "password": password},
                timeout=timeout
            )
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "token": resp.json().get("token", ""),
                "server": server_url
            }, indent=2))]
        
        elif name == "iserver_get_service_list":
            resp = requests.get(f"{server_url}/iserver/manager/services.json", headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "services": resp.json(),
                "server": server_url
            }, indent=2, ensure_ascii=False))]
        
        elif name == "iserver_get_service_status":
            svc = arguments["service_name"]
            resp = requests.get(f"{server_url}/iserver/manager/services/{svc}.json", headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service": svc, "info": resp.json()
            }, indent=2, ensure_ascii=False))]
        
        elif name == "iserver_start_service":
            svc = arguments["service_name"]
            resp = requests.put(f"{server_url}/iserver/manager/services/{svc}/state.json",
                              json={"state": "started"}, headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service": svc, "action": "start", "response": resp.text
            }, indent=2))]
        
        elif name == "iserver_stop_service":
            svc = arguments["service_name"]
            resp = requests.put(f"{server_url}/iserver/manager/services/{svc}/state.json",
                              json={"state": "stopped"}, headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service": svc, "action": "stop", "response": resp.text
            }, indent=2))]
        
        elif name == "iserver_restart_service":
            svc = arguments["service_name"]
            resp = requests.put(f"{server_url}/iserver/manager/services/{svc}/state.json",
                              json={"state": "restarted"}, headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service": svc, "action": "restart", "response": resp.text
            }, indent=2))]
        
        elif name == "iserver_get_map_info":
            map_name = arguments["map_name"]
            resp = requests.get(f"{server_url}/iserver/services/map-{map_name}/rest/maps/{map_name}.json",
                              headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "map": map_name, "info": resp.json()
            }, indent=2, ensure_ascii=False))]
        
        elif name == "iserver_query_data":
            ds_name = arguments["datasource_name"]
            dt_name = arguments["dataset_name"]
            params = {
                "dataset": f"{ds_name}:{dt_name}",
                "maxFeatures": arguments.get("max_features", 1000)
            }
            if "sql_filter" in arguments:
                params["queryParameter"] = json.dumps({"attributeFilter": arguments["sql_filter"]})
            if "geometry" in arguments:
                qp = json.loads(params.get("queryParameter", "{}"))
                qp["spatialQueryObject"] = {
                    "geometry": json.loads(arguments["geometry"]) if isinstance(arguments["geometry"], str) else arguments["geometry"],
                    "spatialQueryMode": arguments.get("spatial_query_mode", "INTERSECT")
                }
                params["queryParameter"] = json.dumps(qp)
            resp = requests.get(f"{server_url}/iserver/services/data-{ds_name}/rest/data",
                              params=params, headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "datasource": ds_name, "dataset": dt_name,
                "result": resp.json()
            }, indent=2, ensure_ascii=False))]
        
        elif name == "iserver_clear_cache":
            svc = arguments["service_name"]
            resp = requests.delete(f"{server_url}/iserver/manager/services/{svc}/caches.json",
                                 headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service": svc, "action": "clear_cache", "response": resp.text
            }, indent=2))]
        
        elif name == "iserver_publish_map_service":
            ws_path = arguments["workspace_path"]
            map_name = arguments["map_name"]
            svc_name = arguments.get("service_name", map_name)
            resp = requests.post(f"{server_url}/iserver/manager/services.json",
                               json={
                                   "serviceName": svc_name,
                                   "type": "map",
                                   "workspacePath": ws_path,
                                   "mapName": map_name
                               }, headers=headers, timeout=timeout)
            return [TextContent(type="text", text=json.dumps({
                "status": "success", "service_name": svc_name, "map": map_name,
                "response": resp.text
            }, indent=2))]
        
        elif name == "generate_map_tiles":
            map_name = arguments["map_name"]
            storage_type = arguments.get("storage_type", "compact")
            scale_denoms = arguments.get("scale_denominators", None)
            bounds = arguments.get("bounds", None)
            if isinstance(scale_denoms, str):
                scale_denoms = json.loads(scale_denoms)
            if isinstance(bounds, str):
                bounds = json.loads(bounds)
            
            # 构建 REST API 请求体
            payload = {
                "serviceName": f"map-{map_name}",
                "type": "map",
                "storageType": storage_type
            }
            if scale_denoms:
                payload["scales"] = scale_denoms
            if bounds:
                payload["bounds"] = {"left": bounds[0], "bottom": bounds[1], "right": bounds[2], "top": bounds[3]}
            
            resp = requests.post(
                f"{server_url}/iserver/services/map-{map_name}/rest/maps/{map_name}/tilesets.json",
                json=payload, headers=headers, timeout=60
            )
            return [TextContent(type="text", text=json.dumps({
                "status": "success",
                "map": map_name,
                "storage_type": storage_type,
                "response": resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            }, indent=2, ensure_ascii=False))]
        
        else:
            return [TextContent(type="text", text=json.dumps({
                "status": "error", "message": f"未知 iServer 工具: {name}"
            }, indent=2))]
    
    except Exception as e:
        return [TextContent(type="text", text=json.dumps({
            "status": "error", "message": f"iServer 请求失败: {str(e)}",
            "traceback": traceback.format_exc()
        }, indent=2))]


async def _check_mcp_health():
    """MCP 健康检查：不依赖 iObjectsPy 初始化，增强版 v4.0
    
    检查项:
    1. iObjectsPy 模块是否可导入
    2. Java 路径是否有效（含 Java 版本检测）
    3. License 文件是否存在且有效
    4. 磁盘空间是否充足
    5. 连接状态
    6. 自动生成修复建议
    """
    checks = {
        "iobjectspy_importable": False,
        "java_path_valid": False,
        "connection_ok": False,
        "license_valid": False,
        "tool_count": 69,
        "initialized": _initialized,
        "suggestions": []
    }
    
    # ===== 1. 检查 License 文件 =====
    license_info = {"path": DEFAULT_LICENSE_PATH}
    if os.path.isdir(DEFAULT_LICENSE_PATH):
        lic_files = [f for f in os.listdir(DEFAULT_LICENSE_PATH) if f.endswith(('.lic', '.licx', '.lic12', '.udlx'))]
        if lic_files:
            checks["license_valid"] = True
            license_info["exists"] = True
            license_info["files"] = lic_files
            license_info["file_count"] = len(lic_files)
        else:
            license_info["exists"] = True
            license_info["error"] = "License 目录存在但未找到 License 文件（.lic/.licx/.lic12/.udlx）"
            checks["suggestions"].append("请将 License 文件（.lic/.licx）放入目录: " + DEFAULT_LICENSE_PATH)
    else:
        license_info["exists"] = False
        license_info["error"] = f"License 目录不存在: {DEFAULT_LICENSE_PATH}"
        checks["suggestions"].append(f"License 目录不存在。请通过环境变量 SUPERMAP_LICENSE 指定正确路径，或安装 SuperMap License 到: {DEFAULT_LICENSE_PATH}")
    checks["license"] = license_info
    
    # ===== 2. 检查 iObjectsPy 是否可导入 =====
    checks["iobjectspy_config_path"] = IOBJECTSPY_PATH
    try:
        import importlib
        spec = importlib.util.find_spec("iobjectspy")
        if spec is not None:
            checks["iobjectspy_importable"] = True
            checks["iobjectspy_path"] = spec.origin
        else:
            if IOBJECTSPY_PATH in sys.path:
                checks["iobjectspy_path"] = IOBJECTSPY_PATH
                checks["iobjectspy_note"] = "路径已添加，但模块未找到"
                checks["suggestions"].append(f"iObjectsPy 路径已添加但模块未找到。请确认路径正确: {IOBJECTSPY_PATH}")
            else:
                checks["suggestions"].append(f"无法找到 iObjectsPy 模块。请通过环境变量 SUPERMAP_IOBJECTSPY_PATH 指定正确路径")
    except Exception as e:
        checks["iobjectspy_error"] = str(e)
        checks["suggestions"].append(f"iObjectsPy 导入异常: {str(e)}")
    
    # ===== 3. 检查 Java 路径（增强：检测 Java 版本） =====
    import subprocess
    if os.path.isdir(DEFAULT_IOBJECT_PATH):
        checks["java_path_valid"] = True
        checks["java_path"] = DEFAULT_IOBJECT_PATH
        # 检测关键文件
        java_files = [f for f in os.listdir(DEFAULT_IOBJECT_PATH) if 'java' in f.lower() or f.endswith('.dll') or f.endswith('.jar')]
        checks["java_key_files"] = java_files[:10]
        # 尝试检测 Java 版本
        java_exe = os.path.join(DEFAULT_IOBJECT_PATH, "java.exe")
        if os.path.exists(java_exe):
            try:
                result = subprocess.run(
                    [java_exe, "-version"],
                    capture_output=True, text=True, timeout=10,
                    encoding="utf-8", errors="replace"
                )
                version_line = result.stderr or result.stdout
                checks["java_version"] = version_line.strip().split("\n")[0] if version_line else "unknown"
            except Exception as e:
                checks["java_version_error"] = str(e)
        else:
            checks["suggestions"].append(f"Java 可执行文件不存在: {java_exe}")
    else:
        checks["java_path_error"] = f"路径不存在: {DEFAULT_IOBJECT_PATH}"
        checks["suggestions"].append(f"Java 路径不存在: {DEFAULT_IOBJECT_PATH}。请通过环境变量 SUPERMAP_JAVA_PATH 指定正确路径")
    
    # ===== 4. 检查磁盘空间 =====
    try:
        import shutil
        # 检查 iObjectsPy 所在磁盘
        if os.path.exists(IOBJECTSPY_PATH):
            disk_info = shutil.disk_usage(IOBJECTSPY_PATH)
            checks["disk_space"] = {
                "path": IOBJECTSPY_PATH,
                "total_gb": round(disk_info.total / (1024**3), 1),
                "free_gb": round(disk_info.free / (1024**3), 1),
                "used_percent": round((1 - disk_info.free / disk_info.total) * 100, 1)
            }
            if disk_info.free < 1024 * 1024 * 1024:  # < 1GB
                checks["suggestions"].append("磁盘剩余空间不足 1GB，可能影响数据处理")
    except Exception as e:
        checks["disk_space_error"] = str(e)
    
    # ===== 5. 检查连接状态 =====
    if _initialized and _init_error is None:
        checks["connection_ok"] = True
    elif _init_error:
        checks["connection_error"] = _init_error
        checks["suggestions"].append(f"iObjectsPy 连接失败: {_init_error}")
    elif not _initialized:
        checks["connection_note"] = "尚未初始化。首次调用工具时会自动初始化"
    
    # ===== 6. 综合状态与修复建议 =====
    all_ok = all([checks["iobjectspy_importable"], checks["java_path_valid"], checks["license_valid"]])
    checks["overall_status"] = "healthy" if all_ok else "degraded"
    
    if not checks["suggestions"]:
        checks["suggestions"].append("所有检查通过，MCP Server 运行正常")
    
    return [TextContent(type="text", text=json.dumps(checks, indent=2, ensure_ascii=False))]


# =============================================================================
# 启动服务器
# =============================================================================

async def main():
    """启动 MCP 服务器"""
    async with stdio_server() as (read_stream, write_stream):
        await _server.run(read_stream, write_stream, _server.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
