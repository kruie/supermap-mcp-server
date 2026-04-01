# SuperMap iDesktopX MCP Server

基于 SuperMap iObjectsPy 的 MCP 服务器，提供 **69 个 GIS 自动化工具**。

## 版本

**v4.2** - JVM 后台预热（消除首次调用冷启动超时）

## 功能分类

| 类别 | 工具数量 | 说明 |
|------|----------|------|
| 初始化与环境 | 3 | initialize_supermap, get_environment_info, check_mcp_health |
| 数据源管理 | 3 | open/create UDBX datasource, memory datasource |
| 工作空间 | 3 | open_workspace, save_workspace, get_workspace_info |
| 坐标系 | 2 | get_coordinate_system, reproject_dataset |
| 数据集管理 | 9 | list/get/query/delete/create/copy/append dataset, add/calculate field |
| 数据导入 | 9 | shapefile, GDB, CSV, TIFF, DWG, KML, GeoJSON, OSM, 批量导入 |
| 数据导出 | 4 | shapefile, GeoJSON, TIFF, 批量导出 |
| 几何转换 | 3 | 点→线, 线→面, 面→线 |
| 空间分析 | 4 | buffer, multi_buffer, overlay, clip_data |
| 融合分析 | 1 | dissolve |
| 栅格分析 | 3 | slope, aspect, hillshade |
| 插值分析 | 2 | IDW, Kriging |
| 密度分析 | 1 | kernel_density |
| 水文分析 | 2 | fill_sink, watershed |
| 其他分析 | 3 | thiessen_polygons, aggregate_points, reclassify |
| 地图制图 | 6 | create/list/get map, add_layer, export_image, generate_tiles |
| 工具函数 | 2 | compute_distance, compute_geodesic_area |
| iServer REST | 10 | 服务管理、查询、缓存、发布、Token |

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

## 许可证

MIT
