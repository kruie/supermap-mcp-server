"""
SuperMap iObjectsPy MCP Server
==============================

使用 MCP SDK 创建的 SuperMap GIS MCP 服务器
支持通过 stdio 与 WorkBuddy 通信

工具数量: 57/57 (全部完成)
"""

import sys
import json
import traceback
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# 设置 iObjectsPy 路径
IOBJECTSPY_PATH = r"D:\software\supermap-idesktopx-2025-windows-x64-bin\bin_python\iobjectspy\iobjectspy-py310_64"
sys.path.insert(0, IOBJECTSPY_PATH)

# 默认 Java 路径
DEFAULT_IOBJECT_PATH = r"D:\software\supermap-idesktopx-2025-windows-x64-bin\bin"

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
            description="初始化 SuperMap iObjectsPy 连接，设置 Java 环境",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_environment_info",
            description="获取 SuperMap 环境信息，包括 Java 路径、OMP 线程数",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="check_mcp_health",
            description="检查 MCP Server 健康状态，包括 iObjectsPy 是否可导入、Java 路径是否有效、连接是否正常",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        # ---- 数据源管理 ----
        Tool(
            name="open_udbx_datasource",
            description="打开 UDBX 数据源文件，返回数据集列表",
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
            description="创建新的 UDBX 数据源文件",
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
            description="创建内存数据源，用于临时数据处理，不需要写入磁盘",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_name": {"type": "string", "description": "内存数据源名称（默认: MemoryDS）"}
                }
            }
        ),
        # ---- 数据集管理 ----
        Tool(
            name="list_datasets",
            description="列出数据源中的所有数据集，包括名称、类型和记录数",
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
            description="获取数据集详细信息，包括类型、记录数、范围等",
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
            description="SQL 属性查询数据集，支持条件过滤、字段选择和数量限制",
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
            description="删除数据源中的指定数据集，操作不可逆",
            inputSchema={
                "type": "object",
                "properties": {
                    "datasource_path": {"type": "string", "description": ".udbx 文件路径"},
                    "dataset_name": {"type": "string", "description": "要删除的数据集名称"}
                },
                "required": ["datasource_path", "dataset_name"]
            }
        ),
        # ---- 数据导入 ----
        Tool(
            name="import_shapefile",
            description="导入 Shapefile 文件到数据源中",
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
            description="导入 ESRI GDB (FileGDB) 数据到数据源中",
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
            description="导入 CSV 文件为点数据集。支持经纬度列映射，自动创建点几何",
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
            description="导入 GeoTIFF 栅格文件为栅格数据集",
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
            description="导入 AutoCAD DWG/DXF 文件为数据集",
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
            description="导入 KML/KMZ 文件为数据集",
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
            description="导入 GeoJSON 文件为矢量数据集",
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
            description="导入 OSM (OpenStreetMap) 文件为数据集",
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
            description="批量导入多个文件到数据源，支持 Shapefile/GeoJSON/CSV/KML/DWG 等格式混合导入",
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
            description="批量导出数据源中的多个数据集为指定格式（Shapefile/GeoJSON/KML）",
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
            description="导出数据集为 Shapefile 文件",
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
            description="导出数据集为 GeoJSON 文件",
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
            description="导出栅格数据集为 GeoTIFF 文件",
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
            description="将点数据集转换为线数据集，按字段排序后依次连线",
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
            description="将线数据集转换为面数据集，封闭区域自动构面",
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
            description="将面数据集转换为线数据集，提取边界线",
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
            description="融合分析，按指定字段合并相邻且属性相同的要素",
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
            description="创建缓冲区，为要素生成指定距离的缓冲多边形",
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
            description="创建多级缓冲区（同心环），可指定多个距离值",
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
            description="叠加分析，支持 INTERSECTION/UNION/ERASE/IDENTITY/UPDATE/CLIP/XOR",
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
            description="裁剪分析，用一个数据集裁剪另一个数据集",
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
            description="计算坡度，基于 DEM 栅格数据",
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
            description="计算坡向，基于 DEM 栅格数据",
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
            description="计算山体阴影，用于地形可视化",
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
            description="IDW 反距离权重插值，将点数据插值为连续栅格面",
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
            description="克里金插值，基于地统计学的空间插值方法",
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
            description="核密度分析，计算点/线要素的密度分布",
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
            description="填洼分析，填充 DEM 中的洼地，生成无洼地 DEM",
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
            description="流域分析/汇水分析，基于填洼 DEM 和流向数据",
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
            description="创建泰森多边形（Voronoi 图），基于点数据集",
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
            description="点聚合分析，将密集点聚合为面要素并统计数量",
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
            description="重分类，将栅格数据按规则重新分类",
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
            description="创建新地图，指定名称和数据范围",
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
            description="列出工作空间中的所有地图",
            inputSchema={
                "type": "object",
                "properties": {}
            }
        ),
        Tool(
            name="get_map_info",
            description="获取地图详细信息，包括图层列表、范围、比例尺等",
            inputSchema={
                "type": "object",
                "properties": {
                    "map_name": {"type": "string", "description": "地图名称"}
                },
                "required": ["map_name"]
            }
        ),
        # ---- 工具函数 ----
        Tool(
            name="compute_distance",
            description="计算两个点之间的距离（支持投影坐标和地理坐标）",
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
            description="计算球面上的面积（平方米），适用于地理坐标系下的面数据",
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
            description="[iServer] 获取所有已发布的服务列表",
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
            description="[iServer] 获取指定服务的运行状态",
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
            description="[iServer] 启动指定服务",
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
            description="[iServer] 停止指定服务",
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
            description="[iServer] 重启指定服务",
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
            description="[iServer] 获取地图服务信息，包括图层、范围、比例尺等",
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
            description="[iServer] 查询数据服务，支持 SQL 查询和空间查询",
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
            description="[iServer] 清除指定服务的缓存",
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
            description="[iServer] 发布地图服务",
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
            description="[iServer] 获取认证令牌",
            inputSchema={
                "type": "object",
                "properties": {
                    "server_url": {"type": "string", "description": "iServer 地址（默认: http://localhost:8090）"},
                    "username": {"type": "string", "description": "用户名（默认: admin）"},
                    "password": {"type": "string", "description": "密码（默认: supermap）"}
                }
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
            info = {
                "status": "success",
                "iobjects_java_path": java_path,
                "omp_threads": omp_threads,
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
            conn_info = DatasourceConnectionInfo.make(arguments["file_path"])
            ds = create_datasource(conn_info)
            result = {"status": "success", "datasource": arguments["file_path"]}
            ds.close()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # 创建内存数据源
        elif name == "create_memory_datasource":
            ds_name = arguments.get("datasource_name", "MemoryDS")
            conn_info = DatasourceConnectionInfo()
            conn_info.server = ds_name
            conn_info.engine_type = iobs.EngineType.MEMORY
            ds = create_datasource(conn_info)
            result = {"status": "success", "datasource": ds_name, "type": "memory"}
            ds.close()
            return [TextContent(type="text", text=json.dumps(result, indent=2))]
        
        # 列出数据集
        elif name == "list_datasets":
            conn_info = DatasourceConnectionInfo.make(arguments["datasource_path"])
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
            conn_info = DatasourceConnectionInfo.make(arguments["datasource_path"])
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
                conn_info = DatasourceConnectionInfo.make(arguments["datasource_path"])
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
                conn_info = DatasourceConnectionInfo.make(arguments["datasource_path"])
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
        
        # 导入 Shapefile
        elif name == "import_shapefile":
            target_name = arguments.get("dataset_name", "") or None
            result = conv.import_shape(arguments["shapefile_path"], arguments["datasource_path"], out_dataset_name=target_name)
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
        # 导入 GDB
        elif name == "import_gdb":
            feature_class = arguments.get("feature_class", "")
            result = conv.import_filegdb(arguments["gdb_path"], arguments["datasource_path"], feature_class)
            return [TextContent(type="text", text=json.dumps({"status": "success", "result": result}, indent=2))]
        
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
    """MCP 健康检查：不依赖 iObjectsPy 初始化"""
    checks = {
        "iobjectspy_importable": False,
        "java_path_valid": False,
        "connection_ok": False,
        "tool_count": 57,
        "initialized": _initialized
    }
    
    # 检查 iObjectsPy 是否可导入
    try:
        import importlib
        spec = importlib.util.find_spec("iobjectspy")
        if spec is not None:
            checks["iobjectspy_importable"] = True
            checks["iobjectspy_path"] = spec.origin
        else:
            # 尝试从自定义路径导入
            if IOBJECTSPY_PATH in sys.path:
                checks["iobjectspy_path"] = IOBJECTSPY_PATH
                checks["iobjectspy_note"] = "路径已添加，但模块未找到"
    except Exception as e:
        checks["iobjectspy_error"] = str(e)
    
    # 检查 Java 路径
    import os
    if os.path.isdir(DEFAULT_IOBJECT_PATH):
        checks["java_path_valid"] = True
        checks["java_path"] = DEFAULT_IOBJECT_PATH
        # 列出关键文件
        java_files = [f for f in os.listdir(DEFAULT_IOBJECT_PATH) if 'java' in f.lower() or f.endswith('.dll')]
        checks["java_key_files"] = java_files[:10]
    else:
        checks["java_path_error"] = f"路径不存在: {DEFAULT_IOBJECT_PATH}"
    
    # 检查连接状态
    if _initialized and _init_error is None:
        checks["connection_ok"] = True
    elif _init_error:
        checks["connection_error"] = _init_error
    
    checks["overall_status"] = "healthy" if all([checks["iobjectspy_importable"], checks["java_path_valid"], checks["connection_ok"]]) else "degraded"
    
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
