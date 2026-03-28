"""
SuperMap iObjectsPy MCP Server - Enhanced Version
=================================================
功能完整的SuperMap MCP服务器，支持：
- 数据源管理
- 数据导入导出
- 数据集操作
- 空间分析
- 地图制图
- 三维分析

使用 iobjectspy 包操作 SuperMap iDesktopX
"""

import iobjectspy as iobs
from iobjectspy import (
    DatasourceConnectionInfo, 
    create_datasource, 
    open_datasource,
    close_datasource,
    list_datasources
)
from iobjectspy import conversion as conv
from iobjectspy import analyst as anl
from iobjectspy import data as data_ops
from iobjectspy import mapping
from iobjectspy.ml import utils as ml_utils

# Default iObjects Java path
DEFAULT_IOBJECT_PATH = r"D:\software\supermap-idesktopx-2025-windows-x64-bin\bin"

# =============================================================================
# 初始化与配置
# =============================================================================

def initialize() -> dict:
    """Initialize the SuperMap connection"""
    try:
        iobs.set_iobjects_java_path(DEFAULT_IOBJECT_PATH)
        return {"status": "success", "message": "SuperMap iObjectsPy initialized", "path": DEFAULT_IOBJECT_PATH}
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_environment_info() -> dict:
    """Get SuperMap environment information"""
    try:
        java_path = iobs.env.get_iobjects_java_path()
        omp_threads = iobs.env.get_omp_num_threads()
        memory_mode = iobs.env.is_use_analyst_memory_mode()
        return {
            "status": "success",
            "iobjects_java_path": java_path,
            "omp_threads": omp_threads,
            "analyst_memory_mode": memory_mode
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 数据源管理
# =============================================================================

def create_udbx_datasource(file_path: str, alias: str = None) -> dict:
    """
    Create a new UDBX datasource
    
    Args:
        file_path: Full path for the new .udbx file (e.g., "E:\\data\\myproject.udbx")
        alias: Optional alias name for the datasource
    
    Returns:
        dict with status and datasource info
    """
    try:
        conn_info = DatasourceConnectionInfo.make(file_path)
        ds = create_datasource(conn_info)
        conn = ds.connection_info
        
        result = {
            "status": "success",
            "message": f"Datasource created: {file_path}",
            "datasource": {
                "alias": ds.alias,
                "server": conn.server,
                "type": str(conn.type)
            }
        }
        ds.close()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

def create_memory_datasource(alias: str = "MemoryDS") -> dict:
    """
    Create an in-memory datasource (temporary, not saved to disk)
    
    Args:
        alias: Optional alias name for the datasource
    
    Returns:
        dict with status and datasource info
    """
    try:
        ds = iobs.create_mem_datasource(alias)
        result = {
            "status": "success",
            "message": f"Memory datasource created: {alias}",
            "datasource": {
                "alias": ds.alias,
                "type": "Memory"
            }
        }
        ds.close()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

def open_udbx_datasource(file_path: str) -> dict:
    """
    Open an existing UDBX datasource
    
    Args:
        file_path: Path to the existing .udbx file
    
    Returns:
        dict with status and datasource info
    """
    try:
        conn_info = DatasourceConnectionInfo.make(file_path)
        ds = open_datasource(conn_info)
        conn = ds.connection_info
        
        datasets = []
        for name in ds.datasets:
            ds_info = ds.datasets[name]
            datasets.append({
                "name": name,
                "type": str(ds_info.type),
                "record_count": ds_info.record_count
            })
        
        result = {
            "status": "success",
            "message": f"Datasource opened: {file_path}",
            "datasource": {
                "alias": ds.alias,
                "server": conn.server,
                "type": str(conn.type)
            },
            "datasets": datasets
        }
        ds.close()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

def list_datasets(file_path: str) -> dict:
    """
    List all datasets in a datasource
    
    Args:
        file_path: Path to the .udbx file
    
    Returns:
        dict with list of datasets
    """
    try:
        conn_info = DatasourceConnectionInfo.make(file_path)
        ds = open_datasource(conn_info)
        
        datasets = []
        for name in ds.datasets:
            ds_info = ds.datasets[name]
            datasets.append({
                "name": name,
                "type": str(ds_info.type),
                "record_count": ds_info.record_count,
                "bounds": str(ds_info.bounds) if hasattr(ds_info, 'bounds') else None
            })
        
        ds.close()
        
        return {
            "status": "success",
            "count": len(datasets),
            "datasets": datasets
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_dataset_info(file_path: str, dataset_name: str) -> dict:
    """
    Get detailed information about a specific dataset
    
    Args:
        file_path: Path to the .udbx file
        dataset_name: Name of the dataset
    
    Returns:
        dict with dataset details
    """
    try:
        conn_info = DatasourceConnectionInfo.make(file_path)
        ds = open_datasource(conn_info)
        
        if dataset_name not in ds.datasets:
            ds.close()
            return {"status": "error", "message": f"Dataset '{dataset_name}' not found"}
        
        ds_info = ds.datasets[dataset_name]
        
        # Get field information
        fields = []
        if hasattr(ds_info, 'fields'):
            for field in ds_info.fields:
                fields.append({
                    "name": field.name,
                    "type": str(field.type),
                    "caption": field.caption if hasattr(field, 'caption') else None
                })
        
        result = {
            "status": "success",
            "dataset": {
                "name": ds_info.name,
                "type": str(ds_info.type),
                "record_count": ds_info.record_count,
                "bounds": str(ds_info.bounds) if hasattr(ds_info, 'bounds') else None,
                "fields": fields
            }
        }
        
        ds.close()
        return result
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 数据导入
# =============================================================================

def import_shapefile(source_path: str, target_datasource_path: str, 
                     target_name: str = None, encoding: str = "UTF-8") -> dict:
    """
    Import Shapefile (.shp) into UDBX datasource
    
    Args:
        source_path: Path to the .shp file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
        encoding: File encoding (default: UTF-8)
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_shape(
            source_path, 
            target_datasource_path, 
            target_name=target_name,
            encoding=encoding
        )
        return {
            "status": "success",
            "message": f"Shapefile imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_csv(source_path: str, target_datasource_path: str,
               target_name: str = None, x_field: str = None, 
               y_field: str = None, encoding: str = "UTF-8") -> dict:
    """
    Import CSV file with coordinates into UDBX datasource
    
    Args:
        source_path: Path to the .csv file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
        x_field: Name of the X coordinate field
        y_field: Name of the Y coordinate field
        encoding: File encoding
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_csv(
            source_path,
            target_datasource_path,
            target_name=target_name,
            x_field=x_field,
            y_field=y_field,
            encoding=encoding
        )
        return {
            "status": "success",
            "message": f"CSV imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_tiff(source_path: str, target_datasource_path: str,
                target_name: str = None) -> dict:
    """
    Import GeoTIFF (.tif) raster into UDBX datasource
    
    Args:
        source_path: Path to the .tif file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_tif(source_path, target_datasource_path, target_name=target_name)
        return {
            "status": "success",
            "message": f"GeoTIFF imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_dwg(source_path: str, target_datasource_path: str,
               target_name: str = None, encoding: str = "GBK") -> dict:
    """
    Import DWG (AutoCAD) file into UDBX datasource
    
    Args:
        source_path: Path to the .dwg file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
        encoding: File encoding
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_dwg(source_path, target_datasource_path, 
                                target_name=target_name, encoding=encoding)
        return {
            "status": "success",
            "message": f"DWG imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_kml(source_path: str, target_datasource_path: str,
               target_name: str = None) -> dict:
    """
    Import KML/KMZ file into UDBX datasource
    
    Args:
        source_path: Path to the .kml or .kmz file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
    
    Returns:
        dict with import status
    """
    try:
        if source_path.lower().endswith('.kmz'):
            result = conv.import_kmz(source_path, target_datasource_path, target_name=target_name)
        else:
            result = conv.import_kml(source_path, target_datasource_path, target_name=target_name)
        return {
            "status": "success",
            "message": f"KML/KMZ imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_geojson(source_path: str, target_datasource_path: str,
                   target_name: str = None, encoding: str = "UTF-8") -> dict:
    """
    Import GeoJSON file into UDBX datasource
    
    Args:
        source_path: Path to the .geojson or .json file
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the imported dataset
        encoding: File encoding
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_geojson(source_path, target_datasource_path, 
                                    target_name=target_name, encoding=encoding)
        return {
            "status": "success",
            "message": f"GeoJSON imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def import_osm(source_path: str, target_datasource_path: str,
               data_type: str = "ALL") -> dict:
    """
    Import OpenStreetMap (.osm) data into UDBX datasource
    
    Args:
        source_path: Path to the .osm file
        target_datasource_path: Path to the target .udbx file
        data_type: Type of data to import (ALL, NODE, WAY, RELATION)
    
    Returns:
        dict with import status
    """
    try:
        result = conv.import_osm(source_path, target_datasource_path, data_type=data_type)
        return {
            "status": "success",
            "message": f"OSM imported: {source_path} -> {target_datasource_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 数据导出
# =============================================================================

def export_to_shapefile(source_datasource_path: str, source_dataset_name: str,
                        target_path: str, encoding: str = "UTF-8") -> dict:
    """
    Export dataset to Shapefile format
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the dataset to export
        target_path: Path for the output .shp file
        encoding: File encoding
    
    Returns:
        dict with export status
    """
    try:
        result = conv.export_to_shape(source_datasource_path, source_dataset_name,
                                     target_path, encoding=encoding)
        return {
            "status": "success",
            "message": f"Exported to Shapefile: {source_dataset_name} -> {target_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def export_to_geojson(source_datasource_path: str, source_dataset_name: str,
                       target_path: str, encoding: str = "UTF-8") -> dict:
    """
    Export dataset to GeoJSON format
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the dataset to export
        target_path: Path for the output .geojson file
        encoding: File encoding
    
    Returns:
        dict with export status
    """
    try:
        result = conv.export_to_geojson(source_datasource_path, source_dataset_name,
                                       target_path, encoding=encoding)
        return {
            "status": "success",
            "message": f"Exported to GeoJSON: {source_dataset_name} -> {target_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def export_to_tiff(source_datasource_path: str, source_dataset_name: str,
                   target_path: str) -> dict:
    """
    Export raster dataset to GeoTIFF format
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the raster dataset to export
        target_path: Path for the output .tif file
    
    Returns:
        dict with export status
    """
    try:
        result = conv.export_to_tif(source_datasource_path, source_dataset_name, target_path)
        return {
            "status": "success",
            "message": f"Exported to GeoTIFF: {source_dataset_name} -> {target_path}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 数据集操作
# =============================================================================

def dataset_point_to_line(source_datasource_path: str, source_dataset_name: str,
                          target_datasource_path: str, target_name: str = None,
                          order_field: str = None) -> dict:
    """
    Convert point dataset to line dataset
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the point dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
        order_field: Field to order points for line creation
    
    Returns:
        dict with operation status
    """
    try:
        result = data_ops.dataset_point_to_line(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name,
            order_field=order_field
        )
        return {
            "status": "success",
            "message": f"Point to Line: {source_dataset_name} -> {target_name or 'new_line_dataset'}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def dataset_line_to_region(source_datasource_path: str, source_dataset_name: str,
                           target_datasource_path: str, target_name: str = None) -> dict:
    """
    Convert line dataset to region (polygon) dataset
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the line dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = data_ops.dataset_line_to_region(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Line to Region: {source_dataset_name} -> {target_name or 'new_region_dataset'}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def dataset_region_to_line(source_datasource_path: str, source_dataset_name: str,
                          target_datasource_path: str, target_name: str = None) -> dict:
    """
    Convert region (polygon) dataset to line dataset
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the region dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = data_ops.dataset_region_to_line(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Region to Line: {source_dataset_name} -> {target_name or 'new_line_dataset'}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def dissolve(source_datasource_path: str, source_dataset_name: str,
             target_datasource_path: str, target_name: str = None,
             dissolve_field: str = None) -> dict:
    """
    Dissolve polygons based on attribute field
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the region dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
        dissolve_field: Field to dissolve on
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.dissolve(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name,
            dissolve_field=dissolve_field
        )
        return {
            "status": "success",
            "message": f"Dissolve: {source_dataset_name} -> {target_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 几何操作
# =============================================================================

def create_buffer(source_datasource_path: str, source_dataset_name: str,
                  target_datasource_path: str, target_name: str = None,
                  buffer_distance: float = 100, unit: str = "METER") -> dict:
    """
    Create buffer around features
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the source dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
        buffer_distance: Buffer distance value
        unit: Distance unit (METER, KILOMETER, MILE, etc.)
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.create_buffer(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name,
            buffer_distance=buffer_distance, unit=unit
        )
        return {
            "status": "success",
            "message": f"Buffer created: {source_dataset_name}, distance={buffer_distance} {unit}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def create_multi_buffer(source_datasource_path: str, source_dataset_name: str,
                        target_datasource_path: str, target_name: str = None,
                        buffer_distances: list = None, unit: str = "METER") -> dict:
    """
    Create multiple buffers at different distances
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the source dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
        buffer_distances: List of buffer distances
        unit: Distance unit
    
    Returns:
        dict with operation status
    """
    try:
        if buffer_distances is None:
            buffer_distances = [100, 500, 1000]
        result = anl.create_multi_buffer(
            source_datasource_path, source_dataset_name,
            target_datasource_path, target_name=target_name,
            buffer_distances=buffer_distances, unit=unit
        )
        return {
            "status": "success",
            "message": f"Multi-buffer created: {source_dataset_name}, distances={buffer_distances}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def overlay(source_datasource_path: str, source_dataset_name: str,
            overlay_datasource_path: str, overlay_dataset_name: str,
            target_datasource_path: str, target_name: str = None,
            operation: str = "INTERSECTION") -> dict:
    """
    Perform overlay analysis (intersection, union, erase, etc.)
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the source dataset
        overlay_datasource_path: Path to the overlay .udbx file
        overlay_dataset_name: Name of the overlay dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
        operation: Overlay operation (INTERSECTION, UNION, ERASE, IDENTITY, UPDATE, CLIP, XOR)
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.overlay(
            source_datasource_path, source_dataset_name,
            overlay_datasource_path, overlay_dataset_name,
            target_datasource_path, target_name=target_name,
            operation=operation
        )
        return {
            "status": "success",
            "message": f"Overlay ({operation}): {source_dataset_name} ∩ {overlay_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def clip(source_datasource_path: str, source_dataset_name: str,
         clip_datasource_path: str, clip_dataset_name: str,
         target_datasource_path: str, target_name: str = None) -> dict:
    """
    Clip features using another dataset
    
    Args:
        source_datasource_path: Path to the source .udbx file
        source_dataset_name: Name of the source dataset
        clip_datasource_path: Path to the clip .udbx file
        clip_dataset_name: Name of the clip dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.clip(
            source_datasource_path, source_dataset_name,
            clip_datasource_path, clip_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Clipped: {source_dataset_name} by {clip_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 空间分析 - 栅格分析
# =============================================================================

def calculate_slope(dem_datasource_path: str, dem_dataset_name: str,
                    target_datasource_path: str, target_name: str = None,
                    z_factor: float = 1.0, unit: str = "DEGREE") -> dict:
    """
    Calculate slope from DEM
    
    Args:
        dem_datasource_path: Path to the DEM .udbx file
        dem_dataset_name: Name of the DEM dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output slope dataset
        z_factor: Z factor for elevation scaling
        unit: Output unit (DEGREE or PERCENT)
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.calculate_slope(
            dem_datasource_path, dem_dataset_name,
            target_datasource_path, target_name=target_name,
            z_factor=z_factor, unit=unit
        )
        return {
            "status": "success",
            "message": f"Slope calculated: {dem_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def calculate_aspect(dem_datasource_path: str, dem_dataset_name: str,
                     target_datasource_path: str, target_name: str = None) -> dict:
    """
    Calculate aspect (exposure) from DEM
    
    Args:
        dem_datasource_path: Path to the DEM .udbx file
        dem_dataset_name: Name of the DEM dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output aspect dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.calculate_aspect(
            dem_datasource_path, dem_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Aspect calculated: {dem_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def calculate_hillshade(dem_datasource_path: str, dem_dataset_name: str,
                        target_datasource_path: str, target_name: str = None,
                        azimuth: float = 315, altitude: float = 45) -> dict:
    """
    Calculate hillshade from DEM
    
    Args:
        dem_datasource_path: Path to the DEM .udbx file
        dem_dataset_name: Name of the DEM dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output hillshade dataset
        azimuth: Sun azimuth (0-360)
        altitude: Sun altitude (0-90)
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.calculate_hill_shade(
            dem_datasource_path, dem_dataset_name,
            target_datasource_path, target_name=target_name,
            azimuth=azimuth, altitude=altitude
        )
        return {
            "status": "success",
            "message": f"Hillshade calculated: {dem_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 空间分析 - 插值分析
# =============================================================================

def idw_interpolate(point_datasource_path: str, point_dataset_name: str,
                    z_field: str, target_datasource_path: str,
                    target_name: str = None, cell_size: float = 100,
                    search_radius: int = 12, power: float = 2.0) -> dict:
    """
    IDW (Inverse Distance Weighting) interpolation
    
    Args:
        point_datasource_path: Path to the point dataset .udbx file
        point_dataset_name: Name of the point dataset with Z values
        z_field: Field name containing Z values
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output raster dataset
        cell_size: Output raster cell size
        search_radius: Search radius for interpolation
        power: IDW power parameter
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.idw_interpolate(
            point_datasource_path, point_dataset_name,
            z_field, target_datasource_path,
            target_name=target_name, cell_size=cell_size,
            search_radius=search_radius, power=power
        )
        return {
            "status": "success",
            "message": f"IDW interpolation: {point_dataset_name}, cell_size={cell_size}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def kriging_interpolate(point_datasource_path: str, point_dataset_name: str,
                        z_field: str, target_datasource_path: str,
                        target_name: str = None, cell_size: float = 100,
                        variogram_model: str = "SPHERICAL") -> dict:
    """
    Kriging interpolation
    
    Args:
        point_datasource_path: Path to the point dataset .udbx file
        point_dataset_name: Name of the point dataset with Z values
        z_field: Field name containing Z values
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output raster dataset
        cell_size: Output raster cell size
        variogram_model: Variogram model type
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.kriging_interpolate(
            point_datasource_path, point_dataset_name,
            z_field, target_datasource_path,
            target_name=target_name, cell_size=cell_size,
            variogram_model=variogram_model
        )
        return {
            "status": "success",
            "message": f"Kriging interpolation: {point_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 空间分析 - 密度分析
# =============================================================================

def kernel_density(point_datasource_path: str, point_dataset_name: str,
                    target_datasource_path: str, target_name: str = None,
                    cell_size: float = 100, search_radius: float = 500) -> dict:
    """
    Kernel density analysis
    
    Args:
        point_datasource_path: Path to the point dataset .udbx file
        point_dataset_name: Name of the point dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output density raster
        cell_size: Output raster cell size
        search_radius: Search radius for density calculation
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.kernel_density(
            point_datasource_path, point_dataset_name,
            target_datasource_path, target_name=target_name,
            cell_size=cell_size, search_radius=search_radius
        )
        return {
            "status": "success",
            "message": f"Kernel density: {point_dataset_name}, radius={search_radius}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 空间分析 - 水文分析
# =============================================================================

def fill_sink(dem_datasource_path: str, dem_dataset_name: str,
              target_datasource_path: str, target_name: str = None) -> dict:
    """
    Fill sinks in DEM for hydrological analysis
    
    Args:
        dem_datasource_path: Path to the DEM .udbx file
        dem_dataset_name: Name of the DEM dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output filled DEM
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.fill_sink(
            dem_datasource_path, dem_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Sinks filled: {dem_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def watershed(dem_datasource_path: str, dem_dataset_name: str,
              pour_point_datasource_path: str, pour_point_dataset_name: str,
              target_datasource_path: str, target_name: str = None) -> dict:
    """
    Watershed analysis
    
    Args:
        dem_datasource_path: Path to the DEM .udbx file
        dem_dataset_name: Name of the DEM dataset
        pour_point_datasource_path: Path to pour point .udbx file
        pour_point_dataset_name: Name of pour point dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output watershed dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.watershed(
            dem_datasource_path, dem_dataset_name,
            pour_point_datasource_path, pour_point_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Watershed calculated for: {pour_point_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 空间分析 - 其他分析
# =============================================================================

def create_thiessen_polygons(point_datasource_path: str, point_dataset_name: str,
                             target_datasource_path: str, target_name: str = None) -> dict:
    """
    Create Thiessen (Voronoi) polygons from points
    
    Args:
        point_datasource_path: Path to the point dataset .udbx file
        point_dataset_name: Name of the point dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output polygon dataset
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.create_thiessen_polygons(
            point_datasource_path, point_dataset_name,
            target_datasource_path, target_name=target_name
        )
        return {
            "status": "success",
            "message": f"Thiessen polygons created from: {point_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def aggregate_points(point_datasource_path: str, point_dataset_name: str,
                     target_datasource_path: str, target_name: str = None,
                     cell_size: float = 500, aggregation_type: str = "COUNT") -> dict:
    """
    Aggregate points into grid cells
    
    Args:
        point_datasource_path: Path to the point dataset .udbx file
        point_dataset_name: Name of the point dataset
        target_datasource_path: Path to the target .udbx file
        target_name: Optional name for the output polygon dataset
        cell_size: Grid cell size
        aggregation_type: Type of aggregation (COUNT, SUM, MEAN, etc.)
    
    Returns:
        dict with operation status
    """
    try:
        result = anl.aggregate_points(
            point_datasource_path, point_dataset_name,
            target_datasource_path, target_name=target_name,
            cell_size=cell_size, aggregation_type=aggregation_type
        )
        return {
            "status": "success",
            "message": f"Points aggregated: {point_dataset_name}, cell_size={cell_size}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 地图制图
# =============================================================================

def create_map(workspace_path: str = None, map_name: str = "NewMap") -> dict:
    """
    Create a new map in workspace
    
    Args:
        workspace_path: Optional path to .sxwu workspace file
        map_name: Name for the new map
    
    Returns:
        dict with operation status
    """
    try:
        result = mapping.add_map(workspace_path, map_name)
        return {
            "status": "success",
            "message": f"Map created: {map_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def list_maps(workspace_path: str = None) -> dict:
    """
    List all maps in workspace
    
    Args:
        workspace_path: Optional path to .sxwu workspace file
    
    Returns:
        dict with list of maps
    """
    try:
        maps = mapping.list_maps(workspace_path)
        return {
            "status": "success",
            "count": len(maps) if maps else 0,
            "maps": list(maps) if maps else []
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def get_map_info(workspace_path: str = None, map_name: str = None) -> dict:
    """
    Get map information
    
    Args:
        workspace_path: Optional path to .sxwu workspace file
        map_name: Name of the map
    
    Returns:
        dict with map information
    """
    try:
        result = mapping.get_map(workspace_path, map_name)
        return {
            "status": "success",
            "message": f"Map info: {map_name}",
            "map": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# 工具函数
# =============================================================================

def compute_distance(point1_x: float, point1_y: float,
                     point2_x: float, point2_y: float,
                     unit: str = "METER") -> dict:
    """
    Compute distance between two points
    
    Args:
        point1_x: X coordinate of first point
        point1_y: Y coordinate of first point
        point2_x: X coordinate of second point
        point2_y: Y coordinate of second point
        unit: Distance unit
    
    Returns:
        dict with distance result
    """
    try:
        distance = data_ops.compute_distance(
            point1_x, point1_y, point2_x, point2_y, unit=unit
        )
        return {
            "status": "success",
            "distance": distance,
            "unit": unit,
            "from": {"x": point1_x, "y": point1_y},
            "to": {"x": point2_x, "y": point2_y}
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

def compute_geodesic_area(region_datasource_path: str, region_dataset_name: str,
                           area_field: str = None, unit: str = "SQUARE_METER") -> dict:
    """
    Compute geodesic area of polygons
    
    Args:
        region_datasource_path: Path to the region dataset .udbx file
        region_dataset_name: Name of the region dataset
        area_field: Optional field to store the calculated area
        unit: Area unit
    
    Returns:
        dict with operation status
    """
    try:
        result = data_ops.compute_geodesic_area(
            region_datasource_path, region_dataset_name,
            area_field=area_field, unit=unit
        )
        return {
            "status": "success",
            "message": f"Geodesic area computed: {region_dataset_name}",
            "result": result
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

# =============================================================================
# MCP Tool Definitions
# =============================================================================

TOOLS = [
    # 初始化
    {
        "name": "initialize_supermap",
        "description": "Initialize the SuperMap iObjectsPy connection",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    {
        "name": "get_environment_info",
        "description": "Get SuperMap environment information (Java path, threads, memory mode)",
        "input_schema": {
            "type": "object",
            "properties": {}
        }
    },
    
    # 数据源管理
    {
        "name": "create_udbx_datasource",
        "description": "Create a new SuperMap UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Full path for the new .udbx file (e.g., 'E:\\data\\project.udbx')"
                },
                "alias": {
                    "type": "string",
                    "description": "Optional alias name for the datasource"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "create_memory_datasource",
        "description": "Create an in-memory datasource (temporary, not saved to disk)",
        "input_schema": {
            "type": "object",
            "properties": {
                "alias": {
                    "type": "string",
                    "description": "Optional alias name for the datasource"
                }
            }
        }
    },
    {
        "name": "open_udbx_datasource",
        "description": "Open an existing SuperMap UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the existing .udbx file"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "list_datasets",
        "description": "List all datasets in a SuperMap datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the .udbx file"
                }
            },
            "required": ["file_path"]
        }
    },
    {
        "name": "get_dataset_info",
        "description": "Get detailed information about a specific dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "file_path": {
                    "type": "string",
                    "description": "Path to the .udbx file"
                },
                "dataset_name": {
                    "type": "string",
                    "description": "Name of the dataset"
                }
            },
            "required": ["file_path", "dataset_name"]
        }
    },
    
    # 数据导入
    {
        "name": "import_shapefile",
        "description": "Import Shapefile (.shp) into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .shp file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: UTF-8)"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_csv",
        "description": "Import CSV file with coordinates into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .csv file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                },
                "x_field": {
                    "type": "string",
                    "description": "Name of the X coordinate field"
                },
                "y_field": {
                    "type": "string",
                    "description": "Name of the Y coordinate field"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: UTF-8)"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_tiff",
        "description": "Import GeoTIFF (.tif) raster into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .tif file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_dwg",
        "description": "Import DWG (AutoCAD) file into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .dwg file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: GBK)"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_kml",
        "description": "Import KML/KMZ file into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .kml or .kmz file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_geojson",
        "description": "Import GeoJSON file into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .geojson or .json file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the imported dataset"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: UTF-8)"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    {
        "name": "import_osm",
        "description": "Import OpenStreetMap (.osm) data into UDBX datasource",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_path": {
                    "type": "string",
                    "description": "Path to the .osm file"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "data_type": {
                    "type": "string",
                    "description": "Type of data to import (ALL, NODE, WAY, RELATION)"
                }
            },
            "required": ["source_path", "target_datasource_path"]
        }
    },
    
    # 数据导出
    {
        "name": "export_to_shapefile",
        "description": "Export dataset to Shapefile format",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the dataset to export"
                },
                "target_path": {
                    "type": "string",
                    "description": "Path for the output .shp file"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: UTF-8)"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_path"]
        }
    },
    {
        "name": "export_to_geojson",
        "description": "Export dataset to GeoJSON format",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the dataset to export"
                },
                "target_path": {
                    "type": "string",
                    "description": "Path for the output .geojson file"
                },
                "encoding": {
                    "type": "string",
                    "description": "File encoding (default: UTF-8)"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_path"]
        }
    },
    {
        "name": "export_to_tiff",
        "description": "Export raster dataset to GeoTIFF format",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the raster dataset to export"
                },
                "target_path": {
                    "type": "string",
                    "description": "Path for the output .tif file"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_path"]
        }
    },
    
    # 数据集操作
    {
        "name": "dataset_point_to_line",
        "description": "Convert point dataset to line dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                },
                "order_field": {
                    "type": "string",
                    "description": "Field to order points for line creation"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "dataset_line_to_region",
        "description": "Convert line dataset to region (polygon) dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the line dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "dataset_region_to_line",
        "description": "Convert region (polygon) dataset to line dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the region dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "dissolve",
        "description": "Dissolve polygons based on attribute field",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the region dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                },
                "dissolve_field": {
                    "type": "string",
                    "description": "Field to dissolve on"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    
    # 几何操作
    {
        "name": "create_buffer",
        "description": "Create buffer around features",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the source dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                },
                "buffer_distance": {
                    "type": "number",
                    "description": "Buffer distance value"
                },
                "unit": {
                    "type": "string",
                    "description": "Distance unit (METER, KILOMETER, MILE, etc.)"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "create_multi_buffer",
        "description": "Create multiple buffers at different distances",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the source dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                },
                "buffer_distances": {
                    "type": "array",
                    "items": {"type": "number"},
                    "description": "List of buffer distances"
                },
                "unit": {
                    "type": "string",
                    "description": "Distance unit"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "overlay",
        "description": "Perform overlay analysis (intersection, union, erase, etc.)",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the source dataset"
                },
                "overlay_datasource_path": {
                    "type": "string",
                    "description": "Path to the overlay .udbx file"
                },
                "overlay_dataset_name": {
                    "type": "string",
                    "description": "Name of the overlay dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                },
                "operation": {
                    "type": "string",
                    "description": "Overlay operation (INTERSECTION, UNION, ERASE, IDENTITY, UPDATE, CLIP, XOR)"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "overlay_datasource_path", "overlay_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "clip",
        "description": "Clip features using another dataset",
        "input_schema": {
            "type": "object",
            "properties": {
                "source_datasource_path": {
                    "type": "string",
                    "description": "Path to the source .udbx file"
                },
                "source_dataset_name": {
                    "type": "string",
                    "description": "Name of the source dataset"
                },
                "clip_datasource_path": {
                    "type": "string",
                    "description": "Path to the clip .udbx file"
                },
                "clip_dataset_name": {
                    "type": "string",
                    "description": "Name of the clip dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output dataset"
                }
            },
            "required": ["source_datasource_path", "source_dataset_name", "clip_datasource_path", "clip_dataset_name", "target_datasource_path"]
        }
    },
    
    # 栅格分析
    {
        "name": "calculate_slope",
        "description": "Calculate slope from DEM",
        "input_schema": {
            "type": "object",
            "properties": {
                "dem_datasource_path": {
                    "type": "string",
                    "description": "Path to the DEM .udbx file"
                },
                "dem_dataset_name": {
                    "type": "string",
                    "description": "Name of the DEM dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output slope dataset"
                },
                "z_factor": {
                    "type": "number",
                    "description": "Z factor for elevation scaling"
                },
                "unit": {
                    "type": "string",
                    "description": "Output unit (DEGREE or PERCENT)"
                }
            },
            "required": ["dem_datasource_path", "dem_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "calculate_aspect",
        "description": "Calculate aspect (exposure) from DEM",
        "input_schema": {
            "type": "object",
            "properties": {
                "dem_datasource_path": {
                    "type": "string",
                    "description": "Path to the DEM .udbx file"
                },
                "dem_dataset_name": {
                    "type": "string",
                    "description": "Name of the DEM dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output aspect dataset"
                }
            },
            "required": ["dem_datasource_path", "dem_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "calculate_hillshade",
        "description": "Calculate hillshade from DEM",
        "input_schema": {
            "type": "object",
            "properties": {
                "dem_datasource_path": {
                    "type": "string",
                    "description": "Path to the DEM .udbx file"
                },
                "dem_dataset_name": {
                    "type": "string",
                    "description": "Name of the DEM dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output hillshade dataset"
                },
                "azimuth": {
                    "type": "number",
                    "description": "Sun azimuth (0-360)"
                },
                "altitude": {
                    "type": "number",
                    "description": "Sun altitude (0-90)"
                }
            },
            "required": ["dem_datasource_path", "dem_dataset_name", "target_datasource_path"]
        }
    },
    
    # 插值分析
    {
        "name": "idw_interpolate",
        "description": "IDW (Inverse Distance Weighting) interpolation",
        "input_schema": {
            "type": "object",
            "properties": {
                "point_datasource_path": {
                    "type": "string",
                    "description": "Path to the point dataset .udbx file"
                },
                "point_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset with Z values"
                },
                "z_field": {
                    "type": "string",
                    "description": "Field name containing Z values"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output raster dataset"
                },
                "cell_size": {
                    "type": "number",
                    "description": "Output raster cell size"
                },
                "search_radius": {
                    "type": "integer",
                    "description": "Search radius for interpolation"
                },
                "power": {
                    "type": "number",
                    "description": "IDW power parameter"
                }
            },
            "required": ["point_datasource_path", "point_dataset_name", "z_field", "target_datasource_path"]
        }
    },
    {
        "name": "kriging_interpolate",
        "description": "Kriging interpolation",
        "input_schema": {
            "type": "object",
            "properties": {
                "point_datasource_path": {
                    "type": "string",
                    "description": "Path to the point dataset .udbx file"
                },
                "point_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset with Z values"
                },
                "z_field": {
                    "type": "string",
                    "description": "Field name containing Z values"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output raster dataset"
                },
                "cell_size": {
                    "type": "number",
                    "description": "Output raster cell size"
                },
                "variogram_model": {
                    "type": "string",
                    "description": "Variogram model type"
                }
            },
            "required": ["point_datasource_path", "point_dataset_name", "z_field", "target_datasource_path"]
        }
    },
    
    # 密度分析
    {
        "name": "kernel_density",
        "description": "Kernel density analysis",
        "input_schema": {
            "type": "object",
            "properties": {
                "point_datasource_path": {
                    "type": "string",
                    "description": "Path to the point dataset .udbx file"
                },
                "point_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output density raster"
                },
                "cell_size": {
                    "type": "number",
                    "description": "Output raster cell size"
                },
                "search_radius": {
                    "type": "number",
                    "description": "Search radius for density calculation"
                }
            },
            "required": ["point_datasource_path", "point_dataset_name", "target_datasource_path"]
        }
    },
    
    # 水文分析
    {
        "name": "fill_sink",
        "description": "Fill sinks in DEM for hydrological analysis",
        "input_schema": {
            "type": "object",
            "properties": {
                "dem_datasource_path": {
                    "type": "string",
                    "description": "Path to the DEM .udbx file"
                },
                "dem_dataset_name": {
                    "type": "string",
                    "description": "Name of the DEM dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output filled DEM"
                }
            },
            "required": ["dem_datasource_path", "dem_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "watershed",
        "description": "Watershed analysis",
        "input_schema": {
            "type": "object",
            "properties": {
                "dem_datasource_path": {
                    "type": "string",
                    "description": "Path to the DEM .udbx file"
                },
                "dem_dataset_name": {
                    "type": "string",
                    "description": "Name of the DEM dataset"
                },
                "pour_point_datasource_path": {
                    "type": "string",
                    "description": "Path to pour point .udbx file"
                },
                "pour_point_dataset_name": {
                    "type": "string",
                    "description": "Name of pour point dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output watershed dataset"
                }
            },
            "required": ["dem_datasource_path", "dem_dataset_name", "pour_point_datasource_path", "pour_point_dataset_name", "target_datasource_path"]
        }
    },
    
    # 其他分析
    {
        "name": "create_thiessen_polygons",
        "description": "Create Thiessen (Voronoi) polygons from points",
        "input_schema": {
            "type": "object",
            "properties": {
                "point_datasource_path": {
                    "type": "string",
                    "description": "Path to the point dataset .udbx file"
                },
                "point_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output polygon dataset"
                }
            },
            "required": ["point_datasource_path", "point_dataset_name", "target_datasource_path"]
        }
    },
    {
        "name": "aggregate_points",
        "description": "Aggregate points into grid cells",
        "input_schema": {
            "type": "object",
            "properties": {
                "point_datasource_path": {
                    "type": "string",
                    "description": "Path to the point dataset .udbx file"
                },
                "point_dataset_name": {
                    "type": "string",
                    "description": "Name of the point dataset"
                },
                "target_datasource_path": {
                    "type": "string",
                    "description": "Path to the target .udbx file"
                },
                "target_name": {
                    "type": "string",
                    "description": "Optional name for the output polygon dataset"
                },
                "cell_size": {
                    "type": "number",
                    "description": "Grid cell size"
                },
                "aggregation_type": {
                    "type": "string",
                    "description": "Type of aggregation (COUNT, SUM, MEAN, etc.)"
                }
            },
            "required": ["point_datasource_path", "point_dataset_name", "target_datasource_path"]
        }
    },
    
    # 地图制图
    {
        "name": "create_map",
        "description": "Create a new map in workspace",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_path": {
                    "type": "string",
                    "description": "Optional path to .sxwu workspace file"
                },
                "map_name": {
                    "type": "string",
                    "description": "Name for the new map"
                }
            }
        }
    },
    {
        "name": "list_maps",
        "description": "List all maps in workspace",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_path": {
                    "type": "string",
                    "description": "Optional path to .sxwu workspace file"
                }
            }
        }
    },
    {
        "name": "get_map_info",
        "description": "Get map information",
        "input_schema": {
            "type": "object",
            "properties": {
                "workspace_path": {
                    "type": "string",
                    "description": "Optional path to .sxwu workspace file"
                },
                "map_name": {
                    "type": "string",
                    "description": "Name of the map"
                }
            },
            "required": ["map_name"]
        }
    },
    
    # 工具函数
    {
        "name": "compute_distance",
        "description": "Compute distance between two points",
        "input_schema": {
            "type": "object",
            "properties": {
                "point1_x": {
                    "type": "number",
                    "description": "X coordinate of first point"
                },
                "point1_y": {
                    "type": "number",
                    "description": "Y coordinate of first point"
                },
                "point2_x": {
                    "type": "number",
                    "description": "X coordinate of second point"
                },
                "point2_y": {
                    "type": "number",
                    "description": "Y coordinate of second point"
                },
                "unit": {
                    "type": "string",
                    "description": "Distance unit (default: METER)"
                }
            },
            "required": ["point1_x", "point1_y", "point2_x", "point2_y"]
        }
    },
    {
        "name": "compute_geodesic_area",
        "description": "Compute geodesic area of polygons",
        "input_schema": {
            "type": "object",
            "properties": {
                "region_datasource_path": {
                    "type": "string",
                    "description": "Path to the region dataset .udbx file"
                },
                "region_dataset_name": {
                    "type": "string",
                    "description": "Name of the region dataset"
                },
                "area_field": {
                    "type": "string",
                    "description": "Optional field to store the calculated area"
                },
                "unit": {
                    "type": "string",
                    "description": "Area unit (default: SQUARE_METER)"
                }
            },
            "required": ["region_datasource_path", "region_dataset_name"]
        }
    }
]


def handle_tool_call(tool_name: str, arguments: dict = None) -> dict:
    """Handle MCP tool calls"""
    if arguments is None:
        arguments = {}
    
    # 初始化与环境
    if tool_name == "initialize_supermap":
        return initialize()
    elif tool_name == "get_environment_info":
        return get_environment_info()
    
    # 数据源管理
    elif tool_name == "create_udbx_datasource":
        return create_udbx_datasource(
            file_path=arguments.get("file_path"),
            alias=arguments.get("alias")
        )
    elif tool_name == "create_memory_datasource":
        return create_memory_datasource(
            alias=arguments.get("alias", "MemoryDS")
        )
    elif tool_name == "open_udbx_datasource":
        return open_udbx_datasource(
            file_path=arguments.get("file_path")
        )
    elif tool_name == "list_datasets":
        return list_datasets(
            file_path=arguments.get("file_path")
        )
    elif tool_name == "get_dataset_info":
        return get_dataset_info(
            file_path=arguments.get("file_path"),
            dataset_name=arguments.get("dataset_name")
        )
    
    # 数据导入
    elif tool_name == "import_shapefile":
        return import_shapefile(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            encoding=arguments.get("encoding", "UTF-8")
        )
    elif tool_name == "import_csv":
        return import_csv(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            x_field=arguments.get("x_field"),
            y_field=arguments.get("y_field"),
            encoding=arguments.get("encoding", "UTF-8")
        )
    elif tool_name == "import_tiff":
        return import_tiff(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "import_dwg":
        return import_dwg(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            encoding=arguments.get("encoding", "GBK")
        )
    elif tool_name == "import_kml":
        return import_kml(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "import_geojson":
        return import_geojson(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            encoding=arguments.get("encoding", "UTF-8")
        )
    elif tool_name == "import_osm":
        return import_osm(
            source_path=arguments.get("source_path"),
            target_datasource_path=arguments.get("target_datasource_path"),
            data_type=arguments.get("data_type", "ALL")
        )
    
    # 数据导出
    elif tool_name == "export_to_shapefile":
        return export_to_shapefile(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_path=arguments.get("target_path"),
            encoding=arguments.get("encoding", "UTF-8")
        )
    elif tool_name == "export_to_geojson":
        return export_to_geojson(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_path=arguments.get("target_path"),
            encoding=arguments.get("encoding", "UTF-8")
        )
    elif tool_name == "export_to_tiff":
        return export_to_tiff(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_path=arguments.get("target_path")
        )
    
    # 数据集操作
    elif tool_name == "dataset_point_to_line":
        return dataset_point_to_line(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            order_field=arguments.get("order_field")
        )
    elif tool_name == "dataset_line_to_region":
        return dataset_line_to_region(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "dataset_region_to_line":
        return dataset_region_to_line(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "dissolve":
        return dissolve(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            dissolve_field=arguments.get("dissolve_field")
        )
    
    # 几何操作
    elif tool_name == "create_buffer":
        return create_buffer(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            buffer_distance=arguments.get("buffer_distance", 100),
            unit=arguments.get("unit", "METER")
        )
    elif tool_name == "create_multi_buffer":
        return create_multi_buffer(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            buffer_distances=arguments.get("buffer_distances"),
            unit=arguments.get("unit", "METER")
        )
    elif tool_name == "overlay":
        return overlay(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            overlay_datasource_path=arguments.get("overlay_datasource_path"),
            overlay_dataset_name=arguments.get("overlay_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            operation=arguments.get("operation", "INTERSECTION")
        )
    elif tool_name == "clip":
        return clip(
            source_datasource_path=arguments.get("source_datasource_path"),
            source_dataset_name=arguments.get("source_dataset_name"),
            clip_datasource_path=arguments.get("clip_datasource_path"),
            clip_dataset_name=arguments.get("clip_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    
    # 栅格分析
    elif tool_name == "calculate_slope":
        return calculate_slope(
            dem_datasource_path=arguments.get("dem_datasource_path"),
            dem_dataset_name=arguments.get("dem_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            z_factor=arguments.get("z_factor", 1.0),
            unit=arguments.get("unit", "DEGREE")
        )
    elif tool_name == "calculate_aspect":
        return calculate_aspect(
            dem_datasource_path=arguments.get("dem_datasource_path"),
            dem_dataset_name=arguments.get("dem_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "calculate_hillshade":
        return calculate_hillshade(
            dem_datasource_path=arguments.get("dem_datasource_path"),
            dem_dataset_name=arguments.get("dem_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            azimuth=arguments.get("azimuth", 315),
            altitude=arguments.get("altitude", 45)
        )
    
    # 插值分析
    elif tool_name == "idw_interpolate":
        return idw_interpolate(
            point_datasource_path=arguments.get("point_datasource_path"),
            point_dataset_name=arguments.get("point_dataset_name"),
            z_field=arguments.get("z_field"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            cell_size=arguments.get("cell_size", 100),
            search_radius=arguments.get("search_radius", 12),
            power=arguments.get("power", 2.0)
        )
    elif tool_name == "kriging_interpolate":
        return kriging_interpolate(
            point_datasource_path=arguments.get("point_datasource_path"),
            point_dataset_name=arguments.get("point_dataset_name"),
            z_field=arguments.get("z_field"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            cell_size=arguments.get("cell_size", 100),
            variogram_model=arguments.get("variogram_model", "SPHERICAL")
        )
    
    # 密度分析
    elif tool_name == "kernel_density":
        return kernel_density(
            point_datasource_path=arguments.get("point_datasource_path"),
            point_dataset_name=arguments.get("point_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            cell_size=arguments.get("cell_size", 100),
            search_radius=arguments.get("search_radius", 500)
        )
    
    # 水文分析
    elif tool_name == "fill_sink":
        return fill_sink(
            dem_datasource_path=arguments.get("dem_datasource_path"),
            dem_dataset_name=arguments.get("dem_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "watershed":
        return watershed(
            dem_datasource_path=arguments.get("dem_datasource_path"),
            dem_dataset_name=arguments.get("dem_dataset_name"),
            pour_point_datasource_path=arguments.get("pour_point_datasource_path"),
            pour_point_dataset_name=arguments.get("pour_point_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    
    # 其他分析
    elif tool_name == "create_thiessen_polygons":
        return create_thiessen_polygons(
            point_datasource_path=arguments.get("point_datasource_path"),
            point_dataset_name=arguments.get("point_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name")
        )
    elif tool_name == "aggregate_points":
        return aggregate_points(
            point_datasource_path=arguments.get("point_datasource_path"),
            point_dataset_name=arguments.get("point_dataset_name"),
            target_datasource_path=arguments.get("target_datasource_path"),
            target_name=arguments.get("target_name"),
            cell_size=arguments.get("cell_size", 500),
            aggregation_type=arguments.get("aggregation_type", "COUNT")
        )
    
    # 地图制图
    elif tool_name == "create_map":
        return create_map(
            workspace_path=arguments.get("workspace_path"),
            map_name=arguments.get("map_name", "NewMap")
        )
    elif tool_name == "list_maps":
        return list_maps(
            workspace_path=arguments.get("workspace_path")
        )
    elif tool_name == "get_map_info":
        return get_map_info(
            workspace_path=arguments.get("workspace_path"),
            map_name=arguments.get("map_name")
        )
    
    # 工具函数
    elif tool_name == "compute_distance":
        return compute_distance(
            point1_x=arguments.get("point1_x"),
            point1_y=arguments.get("point1_y"),
            point2_x=arguments.get("point2_x"),
            point2_y=arguments.get("point2_y"),
            unit=arguments.get("unit", "METER")
        )
    elif tool_name == "compute_geodesic_area":
        return compute_geodesic_area(
            region_datasource_path=arguments.get("region_datasource_path"),
            region_dataset_name=arguments.get("region_dataset_name"),
            area_field=arguments.get("area_field"),
            unit=arguments.get("unit", "SQUARE_METER")
        )
    
    else:
        return {"status": "error", "message": f"Unknown tool: {tool_name}"}


if __name__ == "__main__":
    import json
    
    print("=" * 60)
    print("SuperMap iObjectsPy MCP Server - Enhanced Version")
    print("=" * 60)
    print("")
    
    # Initialize
    print("[1] Initializing SuperMap connection...")
    result = initialize()
    print(f"    Status: {result['status']}")
    print(f"    Message: {result['message']}")
    
    # Get environment info
    print("")
    print("[2] Getting environment info...")
    result = get_environment_info()
    print(f"    iObjects Java Path: {result.get('iobjects_java_path', 'N/A')}")
    print(f"    OMP Threads: {result.get('omp_threads', 'N/A')}")
    
    # List available tools
    print("")
    print(f"[3] MCP Server Configuration:")
    print(f"    Total Tools: {len(TOOLS)}")
    
    # Categorize tools
    categories = {
        "初始化与环境": [],
        "数据源管理": [],
        "数据导入": [],
        "数据导出": [],
        "数据集操作": [],
        "几何操作": [],
        "栅格分析": [],
        "插值分析": [],
        "密度分析": [],
        "水文分析": [],
        "其他分析": [],
        "地图制图": [],
        "工具函数": []
    }
    
    for tool in TOOLS:
        name = tool["name"]
        if name in ["initialize_supermap", "get_environment_info"]:
            categories["初始化与环境"].append(name)
        elif name in ["create_udbx_datasource", "create_memory_datasource", "open_udbx_datasource", "list_datasets", "get_dataset_info"]:
            categories["数据源管理"].append(name)
        elif name.startswith("import_"):
            categories["数据导入"].append(name)
        elif name.startswith("export_"):
            categories["数据导出"].append(name)
        elif name.startswith("dataset_") or name == "dissolve":
            categories["数据集操作"].append(name)
        elif name in ["create_buffer", "create_multi_buffer", "overlay", "clip"]:
            categories["几何操作"].append(name)
        elif name in ["calculate_slope", "calculate_aspect", "calculate_hillshade"]:
            categories["栅格分析"].append(name)
        elif name in ["idw_interpolate", "kriging_interpolate"]:
            categories["插值分析"].append(name)
        elif name in ["kernel_density"]:
            categories["密度分析"].append(name)
        elif name in ["fill_sink", "watershed"]:
            categories["水文分析"].append(name)
        elif name in ["create_thiessen_polygons", "aggregate_points"]:
            categories["其他分析"].append(name)
        elif name.startswith("create_") or name.startswith("list_") or name.startswith("get_") and "map" in name:
            categories["地图制图"].append(name)
        else:
            categories["工具函数"].append(name)
    
    for cat, tools in categories.items():
        if tools:
            print(f"\n    {cat} ({len(tools)}):")
            for t in tools:
                print(f"      - {t}")
    
    print("")
    print("=" * 60)
    print("MCP Server Ready!")
    print("=" * 60)
