#对爬取结果进行后处理
import pandas
import geopandas
from shapely.geometry import point
from shapely.geometry import linestring
from shapely.geometry import polygon

from common import gcj02utils
from common import osutils

from shutil import copyfile

def is_gis_data(file):
    if "geodoer" in file:
        return False #geodoer为本工程的自定义文件（如参数文件、缓存文件），不是GIS数据

    ext = osutils.get_ext(file)

    if ext not in ["shp", "json"]:
        return False
    
    return True

#将gcj02转成wgs84
def gcj02_to_wgs84(in_fp, out_fp):
    gdf = geopandas.read_file(in_fp)

    # GCJ02转WGS84
    for i in range(0, len(gdf)):
        geom = gdf.geometry[i]  # 获取空间属性，即GeoSeries
        geom_type = geom.geom_type
        
        if geom_type == "Point":
            lng,lat = geom.x, geom.y  # x=117.967657, y=24.472853
            lng,lat = gcj02utils.gcj02towgs84(lng, lat)
            gdf.geometry[i] = point.Point(lng, lat)
            pass
        elif geom_type == "LineString":
            old_pnts = geom.coords #获得坐标串
            new_pnts = [] #新坐标
            for old_pnt in old_pnts:
                lng, lat = old_pnt
                lng, lat = gcj02utils.gcj02towgs84(lng, lat) #转换
                new_pnts.append( (lng, lat) )
            gdf.geometry[i] = linestring.LineString(new_pnts)
            pass
        elif geom_type == "Polygon":
            old_pnts = geom.exterior.coords #获得坐标串
            new_pnts = [] #新坐标
            for old_pnt in old_pnts:
                lng, lat = old_pnt
                lng, lat = gcj02utils.gcj02towgs84(lng, lat) #转换
                new_pnts.append( (lng, lat) )
            gdf.geometry[i] = polygon.Polygon(new_pnts)
            pass
        else:
            return False

    # 设置成WGS84，并保存
    gdf.crs = {'init' :'epsg:4326'}
    gdf.to_file(out_fp, encoding="utf-8")
    return True

#将gcj02转成wgs84（批量）
def gcj02_to_wgs84_batch(in_dir, out_dir, new_ext = "json"):
    all_files = osutils.get_all_file(in_dir)

    for file in all_files:
        if not is_gis_data(file):
            copyfile(file,
                osutils.get_outfp(file, out_dir)
            )
            continue

        try:
            out_fp = osutils.get_outfp(file, out_dir, new_ext)
            gcj02_to_wgs84(file, out_fp)
        except Exception as e:
            print(f"[Error] {e.message}")
            pass
    pass

#格式转换
def format_conversion_batch(in_dir, out_dir, new_ext):
    all_files = osutils.get_all_file(in_dir)

    for file in all_files:
        if not is_gis_data(file):
            continue

        out_fp = osutils.get_outfp(file, out_dir, new_ext)
        gdf = geopandas.read_file(file)
        gdf.to_file(out_fp, encoding="utf-8")

#将in_dir中的矢量数据全部合并成一个文件
def merge_dataset(in_dir, out_fp):
    all_files = osutils.get_all_file(in_dir)
    dataset = []

    for file in all_files:
        if is_gis_data(file):
            dataset.append(file)

    gdf = pandas.concat([
        geopandas.read_file(file) for file in dataset
    ]).pipe(geopandas.GeoDataFrame)
    gdf.to_file(out_fp)
    pass

if __name__ == '__main__':
    in_dir = r"E:\python\DownloadGeoData\上海_gcj02"
    out_dir = r"E:\python\DownloadGeoData\上海_wgs84"
    gcj02_to_wgs84_batch(in_dir, out_dir)
    merge_dataset(in_dir, out_dir + r"\merge.shp")
    pass