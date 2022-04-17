"""
@Author  : geodoer
@Time    :  2020/2/24 17:12
            2022/4/11 23:16 重构
@Email   : geodoer@163.com
@Func    : 行政区域查询
@Desc    : https://lbs.amap.com/api/webservice/guide/api/district
    例如：中国>山东省>济南市>历下区>舜华路街道（国>省>市>区>街道）
"""
from amap_api import defines
from amap_api import utils
from common import urllibagent

params = {
    "key" : defines.AMAP_KEY
    ,"keyword" : "厦门"
}

class AMapRegionAPI(object):
    URL = 'https://restapi.amap.com/v3/config/district'

    def __init__(self, key) -> None:
        self.__key = key
        self.__urllib = urllibagent.UrllibAgent(
            self.__class__.__name__
            ,urllibagent.RequestLimitsRule(50, 1)
            ,urllibagent.RequestLimitsRule(30000, 24 * 60 * 60)
        )

    def get_region(self, keyword, rec_level = 1):
        """按keyword搜索行政区，并获取边界
        :param keyword: 行政区名称、citycode、adcode
        :param rec_level: 递归等级
            0：不返回下级行政区
            1：返回下一级行政区
            2：返回下两级行政区
            3：返回下三级行政区
        
        :return [] 多个匹配结果
        """
        params = {
            'key': self.__key,
            'keywords': keyword,
            'subdistrict' : rec_level,
            'extensions': 'all'
        }

        result = self.__urllib.request(AMapRegionAPI.URL, params)
        
        if utils.require_success(result):
            return result.get("districts", [])

        return[]


    def get_rect(self, keyword):
        """根据keyword搜索行政区，获取其Rect
        :param keyword: 行政区名称、citycode、adcode
        :return: minlng, maxlng, minlat, maxlat（最小经度，最大经度，最小纬度，最大纬度）
        """
        try:
            disctrict = self.get_region(keyword)

            bestresult = disctrict[0]
            polyline = bestresult.get("polyline", None)
            lines = utils.parse_polyline_str(polyline)

            minlng, maxlng, minlat, maxlat = 200, -1, 200, -1
            for line in lines:
                for lng,lat in line:
                    maxlng = max(lng, maxlng)
                    minlng = min(lng, minlng)
                    maxlat = max(lat, maxlat)
                    minlat = min(lat, minlat)

            return minlng, maxlng, minlat, maxlat
        except:
            return []
    
    @staticmethod
    def division_grid(region, col_num, row_num):
        """
        根据范围划分网格，获得每个网格的四边形边界
        返回：经度和纬度用","分割，经度在前，纬度在后，坐标对用"|"分割。经纬度小数点后不得超过6位
            多边形为矩形时，可传入左上右下两顶点坐标对；其他情况下首尾坐标对需相同。
        """
        minlng, maxlng, minlat, maxlat = region

        polylists = []
        lat_step = (maxlat - minlat) / row_num
        lng_step = (maxlng - minlng) / col_num

        for r in range(row_num):
            for c in range(col_num):
                left_lng = minlng + lng_step * c
                right_lng = minlng + lng_step * (c+1)
                down_lat = minlat + lat_step * r
                up_lat = minlat + lat_step * (r+1)
                # 经度lng在前，纬度lat在后。左上右下两顶点坐标对
                poly_str = f"{left_lng},{up_lat}|{right_lng},{down_lat}"
                polylists.append(poly_str)
        return polylists

if __name__ == '__main__':
    region_http = AMapRegionAPI(params["key"])

    obj = region_http.get_region(params["keyword"])
    print(obj)

    obj = region_http.get_rect(params["keyword"])
    print(obj)

    pass