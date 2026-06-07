# SuperMap iDesktopX MCP Server

基于 SuperMap iObjectsPy 的 MCP 服务器，提供 **253 个 GIS 自动化工具**，覆盖数据管理、空间分析、栅格处理、水文分析、倾斜摄影、三维建模、矢量数据处理、三维数据导入、数据管理、地图瓦片、iServer REST API 等完整 GIS 工作流。

## 版本

**v7.5-fix2** - 恢复全部扩展工具（矢量处理+三维导入+数据扩展+数据管理+地图瓦片），修复连接问题

## 功能分类

| 类别 | 工具数量 | 代表工具 |
|------|:--------:|---------|
| 初始化与环境 | 3 | `initialize_supermap`, `get_environment_info`, `check_mcp_health` |
| 数据源管理 | 4 | `open_udbx_datasource`, `create_udbx_datasource`, `create_memory_datasource`, `close_datasource` |
| 工作空间 | 3 | `open_workspace`, `save_workspace`, `get_workspace_info` |
| 坐标系 | 3 | `get_coordinate_system`, `reproject_dataset`, `convert_coordinates` |
| 数据集管理 | 24 | `list/get/query/create/delete/copy/append dataset`, `add/calculate/rename/delete field`, `update/delete record`, 排序、提取 ID 等 |
| 数据导入 | 15+17=**32** | Shapefile、GDB、CSV、TIFF、DWG、KML、GeoJSON、OSM、Excel、GPX、E00、MIF、SDX、JSON、`batch_import` + **DEM/BIL/RAW/BSQ/BIP/GRIB2/VCT/LIDAR/电信/ArcInfo Grid 等** |
| 数据导出 | 11 | Shapefile、GeoJSON、TIFF、KML、CSV、Excel、GDB、DWG、SVG、PNG/JPG、`batch_export` |
| 几何与格式转换 | 8 | 点↔线↔面互转、矢量↔栅格互转、属性表转点 |
| 矢量空间分析 | 24 | 缓冲区、叠加、裁剪、融合、空间连接、邻近分析、凸包、消除碎面、点聚合、数据透视表等 |
| 矢量数据处理（新增） | **23** | **几何属性计算、线拓扑处理、点抽稀、中心线提取、去冗去重、矢量重采样、保护性分解、编码工具等** |
| 栅格分析 | 18 | 坡度/坡向/山体阴影、重分类、NDVI/NDWI、波段运算、栅格计算器、镶嵌、等值线等 |
| 插值与密度分析 | 3 | `idw_interpolate`, `kriging_interpolate`, `kernel_density` |
| 水文分析 | 15 | 填洼、流向、汇水量、流域分割、河流提取、河流分级、刻河等完整水文工作流 |
| 地图制图 | 7 | `create/list/get_map`, `add_layer_to_map`, `export_map_image`, `generate_map_tiles`, `create_strip_map` |
| 工具函数 | 2 | `compute_distance`, `compute_geodesic_area` |
| 倾斜摄影与三维数据处理 | 17 | 倾斜摄影入库 (S3M)、单体化、裁剪、纹理重映射、3D Tiles ↔ S3M 转换、水印等 |
| 三维数据导入（新增） | **11** | **IFC、3DXML、GIM、RVM、CityGML、RVT、DGN、点加模型、GIM 筛选等** |
| 规则建模 (3D 城市建模 / CIM) | 10 | 线性/旋转拉伸、放样、坡屋顶、构建房、道路工程设计、屋顶分类、建筑物边界规范化、一键带屋顶建筑等 |
| iServer REST API | 10 | 服务管理 (start/stop/restart)、查询、发布、清除缓存、Token 获取 |
| 脚本执行与流水线 | 2 | `run_python_script`, `execute_pipeline` |

**合计：264 个工具**

## 工具详细列表

<details>
<summary>初始化与环境 (3)</summary>

| 工具名 | 说明 |
|--------|------|
| `initialize_supermap` | 初始化 SuperMap iObjectsPy 连接 |
| `get_environment_info` | 获取 Java/License 等环境信息 |
| `check_mcp_health` | MCP Server 健康状态检查（增强版，含 Java 版本检测） |

</details>

<details>
<summary>数据源管理 (4)</summary>

| 工具名 | 说明 |
|--------|------|
| `open_udbx_datasource` | 打开 UDBX 数据源文件 |
| `create_udbx_datasource` | 创建新的 UDBX 数据源文件 |
| `create_memory_datasource` | 创建内存数据源（临时处理） |
| `close_datasource` | 关闭数据源连接，释放资源 |

</details>

<details>
<summary>工作空间 (3)</summary>

| 工具名 | 说明 |
|--------|------|
| `open_workspace` | 打开工作空间文件 (.smwu/.sxwu) |
| `save_workspace` | 保存工作空间，支持另存为 |
| `get_workspace_info` | 获取工作空间详细信息 |

</details>

<details>
<summary>坐标系 (3)</summary>

| 工具名 | 说明 |
|--------|------|
| `get_coordinate_system` | 获取数据集坐标系信息（含 EPSG 代码） |
| `reproject_dataset` | 坐标转换（WGS84 → CGCS2000 等） |
| `convert_coordinates` | 批量转换坐标点数据的坐标系 |

</details>

<details>
<summary>数据集管理 (24)</summary>

| 工具名 | 说明 |
|--------|------|
| `list_datasets` | 列出数据源中所有数据集 |
| `get_dataset_info` | 获取数据集详细信息（字段、记录数、范围） |
| `query_dataset` | SQL 属性查询 |
| `create_dataset` | 创建新数据集（点/线/面/文本/属性表） |
| `delete_dataset` | 删除数据集（不可逆） |
| `copy_dataset` | 复制数据集到同/不同数据源 |
| `append_to_dataset` | 将一个数据集追加到另一个数据集 |
| `rename_dataset` | 重命名数据集 |
| `add_field` | 为数据集添加新字段 |
| `calculate_field` | 批量计算字段值（支持表达式） |
| `get_field_info` | 获取字段详细信息列表 |
| `delete_field` | 删除字段（不可逆） |
| `rename_field` | 重命名字段 |
| `update_record` | 更新指定记录的字段值 |
| `delete_record` | 按条件删除记录（不可逆） |
| `delete_by_filter` | 按 SQL 条件批量删除要素 |
| `get_record_count` | 获取数据集记录总数 |
| `get_dataset_bounds` | 获取数据集空间范围（外接矩形） |
| `sort_dataset` | 按字段排序数据集 |
| `extract_object_id` | 提取要素 SmID 和指定字段，生成 ID 对照表 |
| `recalculate_bounds` | 重新计算并更新数据集空间范围 |
| `calculate_envelope` | 计算要素最小外接矩形 |
| `break_vertices` | 节点打断（在相交处打断线要素） |
| `delete_vector_pyramid` | 删除矢量金字塔索引 |

</details>

<details>
<summary>数据导入 (15)</summary>

| 工具名 | 说明 |
|--------|------|
| `import_shapefile` | 导入 Shapefile (.shp) |
| `import_gdb` | 导入 ESRI FileGDB |
| `import_csv` | 导入 CSV（支持经纬度列映射创建点数据） |
| `import_tiff` | 导入 GeoTIFF 栅格文件 |
| `import_dwg` | 导入 AutoCAD DWG/DXF |
| `import_kml` | 导入 KML/KMZ |
| `import_geojson` | 导入 GeoJSON |
| `import_osm` | 导入 OpenStreetMap (.osm) |
| `import_excel` | 导入 Excel（支持经纬度列映射） |
| `import_simple_json` | 导入结构化 JSON（非 GeoJSON） |
| `import_gpx` | 导入 GPX 轨迹文件 |
| `import_e00` | 导入 ArcInfo Coverage E00 |
| `import_mif` | 导入 MapInfo MIF/MID |
| `import_sdx` | 导入 SuperMap SDX+ 空间数据库数据 |
| `batch_import` | 批量导入多个文件（多种格式） |

</details>

<details>
<summary>数据导出 (11)</summary>

| 工具名 | 说明 |
|--------|------|
| `export_shapefile` | 导出为 Shapefile |
| `export_geojson` | 导出为 GeoJSON |
| `export_tiff` | 导出栅格为 GeoTIFF |
| `export_kml` | 导出为 KML |
| `export_csv` | 导出属性表为 CSV |
| `export_excel` | 导出属性表为 Excel |
| `export_gdb` | 导出为 ESRI FileGDB |
| `export_dwg` | 导出为 AutoCAD DWG/DXF |
| `export_svg` | 导出为 SVG 矢量图 |
| `export_png_jpg` | 导出地图为 PNG/JPG |
| `batch_export` | 批量导出多个数据集 |

</details>

<details>
<summary>几何与格式转换 (8)</summary>

| 工具名 | 说明 |
|--------|------|
| `dataset_point_to_line` | 点→线（按字段排序后依次连线） |
| `dataset_line_to_region` | 线→面 |
| `dataset_region_to_line` | 面→线（提取边界） |
| `dataset_region_to_point` | 面→点（质心/内点提取） |
| `dataset_line_to_point` | 线→点（节点/中点提取） |
| `dataset_vector_to_raster` | 矢量→栅格 |
| `dataset_raster_to_vector` | 栅格→矢量 |
| `dataset_tabular_to_point` | 属性表→点（根据经纬度字段） |

</details>

<details>
<summary>矢量空间分析 (24)</summary>

| 工具名 | 说明 |
|--------|------|
| `create_buffer` | 单环缓冲区分析 |
| `create_multi_buffer` | 多级缓冲区（同心环） |
| `overlay` | 叠加分析（INTERSECTION/UNION/ERASE/IDENTITY） |
| `clip_data` | 裁剪分析 |
| `dissolve` | 融合分析（按属性合并相邻同类要素） |
| `spatial_join` | 空间连接（空间关系合并属性） |
| `eliminate` | 消除碎面（合并小多边形） |
| `merge_datasets` | 合并多个同结构数据集 |
| `spatial_query` | 空间查询（包含/相交/邻近等关系） |
| `proximity_analysis` | 邻近分析（查找最近邻及距离） |
| `convex_hull` | 计算凸包 |
| `minimum_bounding_geometry` | 最小外接几何（矩形/圆/凸包） |
| `smooth_line` | 线/面边界光滑（B 样条/贝塞尔） |
| `aggregate_points` | 点聚合（密集点→面要素） |
| `create_thiessen_polygons` | 泰森多边形（Voronoi 图） |
| `merge_slivers_by_filter` | 碎多边形按条件合并 |
| `building_regularization` | 建筑物规则化（直角化） |
| `building_boundary_regularization` | 建筑物边界规范化（正交化） |
| `region_aggregate` | 面聚合（合并相邻面） |
| `point_cluster_to_region` | 点群区域化（密集点→面区域） |
| `count_features_in_region` | 面内对象数量统计 |
| `summary_statistics` | 汇总统计（分组+多种统计量） |
| `data_pivot_table` | 数据透视表（多维交叉统计） |
| `create_vector_pyramid` | 创建矢量金字塔索引 |

</details>

<details>
<summary>栅格分析 (18)</summary>

| 工具名 | 说明 |
|--------|------|
| `calculate_slope` | 计算坡度 |
| `calculate_aspect` | 计算坡向 |
| `calculate_hillshade` | 计算山体阴影 |
| `reclassify` | 重分类（连续值→等级） |
| `raster_resample` | 栅格重采样（改变分辨率） |
| `raster_composite` | 影像合成（多单波段→多波段） |
| `raster_split` | 栅格分割（多波段→单波段） |
| `raster_weighted_sum` | 栅格加权求和（多因子适宜性评价） |
| `calculate_ndvi` | 计算 NDVI（归一化植被指数） |
| `calculate_ndwi` | 计算 NDWI（归一化水体指数） |
| `raster_band_math` | 栅格波段运算（自定义代数运算） |
| `raster_clip` | 栅格裁剪（按面/矩形范围） |
| `raster_aggregate` | 栅格聚合（降低分辨率） |
| `raster_contour` | 提取等值线/轮廓线 |
| `raster_mosaic` | 栅格镶嵌（多幅拼接） |
| `raster_update` | 栅格数据更新（局部更新） |
| `raster_fill_nodata` | 填充无数据像元 |
| `raster_calculator` | 栅格计算器（任意数学表达式） |

</details>

<details>
<summary>插值与密度分析 (3)</summary>

| 工具名 | 说明 |
|--------|------|
| `idw_interpolate` | IDW 反距离权重插值 |
| `kriging_interpolate` | 克里金插值（地统计学） |
| `kernel_density` | 核密度分析（热力图） |

</details>

<details>
<summary>水文分析 (15)</summary>

| 工具名 | 说明 |
|--------|------|
| `fill_sink` | 填洼（DEM 预处理） |
| `watershed` | 流域/汇水范围分析 |
| `calculate_flow_direction` | 计算流向 |
| `calculate_flow_length` | 计算流长 |
| `calculate_accumulation` | 计算汇水量（累积流量） |
| `calculate_pour_points` | 计算汇水点（流域出口） |
| `snap_pour_points` | 捕捉汇水点到高汇水量像元 |
| `watershed_split` | 流域分割为子流域 |
| `calculate_watershed_basin` | 自动提取流域盆地 |
| `extract_stream_network` | 按阈值提取栅格水系 |
| `stream_order` | 河流分级（Strahler/Strickler） |
| `stream_to_vector` | 栅格水系矢量化 |
| `link_streams` | 连接水系河段（赋唯一标识） |
| `burn_streams_to_dem` | 刻河（将矢量河流下切到 DEM） |
| `extract_longest_flow_path` | 提取最长流路径 |

</details>

<details>
<summary>地图制图 (7)</summary>

| 工具名 | 说明 |
|--------|------|
| `create_map` | 创建新地图 |
| `list_maps` | 列出工作空间中所有地图 |
| `get_map_info` | 获取地图图层/范围/比例尺等信息 |
| `add_layer_to_map` | 向地图添加数据集图层 |
| `export_map_image` | 导出地图为 PNG/JPG（支持指定范围和分辨率） |
| `generate_map_tiles` | 生成地图瓦片缓存（[iServer]） |
| `create_strip_map` | 创建带状地图分幅 |

</details>

<details>
<summary>工具函数 (2)</summary>

| 工具名 | 说明 |
|--------|------|
| `compute_distance` | 计算两点距离（投影/地理坐标） |
| `compute_geodesic_area` | 计算球面面积（WGS84/CGCS2000） |

</details>

<details>
<summary>倾斜摄影与三维数据处理 (17)</summary>

| 工具名 | 说明 |
|--------|------|
| `oblique_to_s3m` | 倾斜摄影入库生成 S3M |
| `oblique_to_s3m_single` | 倾斜摄影单体化入库 |
| `oblique_generate_normal` | 生成法线（提升渲染效果） |
| `oblique_modify_center` | 修改倾斜摄影模型中心点 |
| `oblique_clip` | 按面范围裁剪倾斜摄影数据 |
| `generate_oblique_config` | 生成倾斜摄影 SCP 配置文件 |
| `obj_to_osgb` | OBJ → OSGB 格式转换 |
| `oblique_texture_remap` | 倾斜摄影纹理重映射 |
| `tdtiles_to_s3m` | 3D Tiles → S3M 格式转换 |
| `s3m_to_3dtiles` | S3M → 3D Tiles 格式转换 |
| `generate_oblique_index` | 生成倾斜摄影空间索引 |
| `update_oblique_data` | 倾斜摄影数据增量更新 |
| `update_oblique_mongodb` | 倾斜摄影数据更新（MongoDB 存储） |
| `oblique_continue_generate` | 倾斜入库续生成（中断恢复） |
| `oblique_preprocess` | 倾斜数据预处理（格式检查/坐标转换/纹理优化） |
| `extract_oblique_root` | 提取倾斜数据根节点 |
| `set_oblique_watermark` | 设置倾斜数据版权水印 |

</details>

<details>
<summary>规则建模 - 3D 城市建模 / CIM (10)</summary>

| 工具名 | 说明 |
|--------|------|
| `linear_extrude` | 线性拉伸（二维面→三维体） |
| `rotate_extrude` | 旋转拉伸（回转体建模） |
| `extrude_closed_body` | 拉伸闭合体（封闭三维实体） |
| `loft` | 放样（沿路径将截面放样为三维模型） |
| `build_slope_roof` | 构建坡屋顶（人字顶/四坡顶/复杂坡顶） |
| `build_house` | 构建房（自动生成完整房屋三维模型） |
| `road_engineering_design` | 道路工程设计（含路基/路面/边坡的三维道路） |
| `vector_extrude` | 矢量拉伸（点/线/面→三维要素） |
| `roof_classification` | 屋顶分类（自动识别屋顶类型） |
| `build_building_with_roof` | 一键构建带屋顶建筑物（完整流程：规范化→拉伸→屋顶→纹理→输出 S3M/OBJ） |

</details>

<details>
<summary>iServer REST API (10)</summary>

| 工具名 | 说明 |
|--------|------|
| `iserver_get_service_list` | 获取已发布服务列表 |
| `iserver_get_service_status` | 获取服务运行状态 |
| `iserver_start_service` | 启动指定服务 |
| `iserver_stop_service` | 停止指定服务 |
| `iserver_restart_service` | 重启指定服务 |
| `iserver_get_map_info` | 获取地图服务信息 |
| `iserver_query_data` | 查询数据服务（支持 SQL 和空间查询） |
| `iserver_clear_cache` | 清除服务缓存 |
| `iserver_publish_map_service` | 发布地图服务 |
| `iserver_get_token` | 获取认证令牌 |

</details>

<details>
<summary>脚本执行与流水线 (2)</summary>

| 工具名 | 说明 |
|--------|------|
| `run_python_script` | 在 MCP Server 进程内执行自定义 Python 脚本（iObjectsPy 环境已初始化） |
| `execute_pipeline` | 批量串行执行多个 MCP 工具（GIS 自动化流水线） |

</details>

## 安装

将此服务器添加到 WorkBuddy MCP 配置 (`~/.workbuddy/mcp.json`)：

```json
{
  "mcpServers": {
    "supermap-mcp-server": {
      "command": "C:/Users/jia/.workbuddy/binaries/python/versions/3.10.11/python.exe",
      "args": ["C:/Users/jia/.workbuddy/mcp/supermap_mcp_server.py"],
      "env": {
        "SUPERMAP_IOBJECTSPY_PATH": "D:/software/supermap-iobjectspy-2025/iobjectspy/iobjectspy-py310_64",
        "SUPERMAP_IDESKTOPX_BIN": "D:/software/supermap-idesktopx-2025-windows-x64-bin/bin",
        "SUPERMAP_JAVA_PATH": "D:/software/supermap-idesktopx-2025-windows-x64-bin/bin",
        "SUPERMAP_LICENSE": "C:/Program Files/Common Files/SuperMap/License"
      }
    }
  }
}
```

## 依赖

- Python 3.10（64 位）
- SuperMap iObjectsPy 2025（`iobjectspy-py310_64`）
- SuperMap iDesktopX 2025（提供 JRE 和 Wrapj DLL）
- MCP SDK：`pip install mcp`

## 许可证

MIT
