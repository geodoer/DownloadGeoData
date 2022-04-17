from amap_api import defines
from amap_api import region
from amap_api import utils
from common import urllibagent

import os
import json
import jsonpickle

import geopandas
from shapely.geometry import point

poi_params = {
    "key" : defines.AmapConfig["key"]
    
    # 城市
    ,"city" : "上海"
    
    # POI类型（可查看官方给的文档）
    ,"type" : [
       ["商务住宅", "120000"]
    #    ,["汽车服务相关", "010000"]
    ]
    
    #保存的字段。可选，默认为id、name
    ,"save_field" : ["id", "name", "type", "typecode", "address", "pname", "cityname"]

    #输出路径（不要后缀名，程序会自动加json）
    ,"out_dir": "上海POI_gcj02"

    # 每一次存储到文件的数据量
    ,"number_of_per_time" : 500 

    # 将城市范围的矩形分成row_num行、col_num列的小矩阵
    ,"row_num" : 4
    ,"col_num" : 4
}

class AMapPOIHttp(object):
    URL = 'https://restapi.amap.com/v3/place/polygon'
    params = {}     #AMap POI Http Parameters
    state = {}      #AMap POI Http State
    dataset = {}    #AMap POI Http Result

    """init(Class constructor)
    Download the AMap POI using HTTP

    :param key:             your amap key(See the AMap's website and apply)
    :param city:            your target city
    :param typenamecodes:   your target poi types(See AMap's POI category table)
                    a list of pair, pair is ('<typename>', '<typecode>')
                    [
                        ("商务住宅", "120000")
                        ,("汽车服务相关", "010000")
                        ,...
                    ]
    :param save_field:      The fields you need to save(See AMap's HTPP interface description)
    :param out_dir:         The path to the output folder
    :param num_row,num_col: the number of rows and columns in a city
    :param num_per_flush:   When the number of POIs reaches num_per_flush, the cache is flushed
    :param load_cache:      If there is a cache, the state is loaded from the cache
    """
    def __init__(self
        , amap_key 
        , city
        , typenamecodes = None
        , save_field =  ["id", "name"]
        , out_dir = "poi"
        , num_row = 4
        , num_col = 4
        , num_per_save = 500
        , load_cache = True
        ):
        
        if load_cache: #State cache, whether rollback
            if os.path.exists(self.__cache_file):
                with open(self.__cache_file, encoding="utf-8") as f:
                    self = jsonpickle.decode(f.read())
                pass    #Roll back the success
        else:
            self.__urllib = urllibagent.UrllibAgent(
                self.__class__.__name__
                ,urllibagent.RequestLimitsRule(50, 1)               #50/秒
                ,urllibagent.RequestLimitsRule(30000, 24 * 60 * 60) #30000/天
            )

            '''
            get parameters
            '''
            self.params["key"] = amap_key
            self.params["city"] = city

            if typenamecodes is None or len(typenamecodes)==0:
                self.params["typenamecodes"] = self.__get_all_type()
            else:
                self.params["typenamecodes"] = typenamecodes

            self.params["save_field"] = save_field
            self.params["out_dir"] = out_dir
            self.params["col_num"] = num_col
            self.params["row_num"] = num_row
            self.params["num_per_save"] = num_per_save

            '''
            init:
            '''
            self.state["over"] = False          #task is over
            self.state["poi_count"] = 0         #Number of downloaded POIs
            self.state["out_file_cnt"] = 0      #Number of output files
            
            ##grid > type > page
            self.__compute_grid()
            self.state["grid_cursor"] = 0
            self.state["typenamecodes_cursor"] = 0
            self.state["poi_page_cursor"] = 0   #poi page index

            self.__reset_dataset()

        if not os.path.exists(self.params["out_dir"]):
            os.makedirs(self.params["out_dir"])

        super(AMapPOIHttp, self).__init__()

    @property
    def __cache_file(self)->str:
        return f"cache-{self.__class__.__name__}.json"

    def start(self):
        """Start the download
        """
        if self.state["over"]:
            self.__success()
            return

        cnt = 0

        #grid
        while self.state["grid_cursor"] < len(self.state["grid"]):
            print("正在第{}个方格区搜索".format(self.state['grid_cursor']))

            poly_str = self.state['grid'][self.state["grid_cursor"]]

            #type
            while self.state["typenamecodes_cursor"] < len(self.params["typenamecodes"]):
                typename, typecode = self.params["typenamecodes"][self.state["typenamecodes_cursor"]]

                #page
                page_over = False #Todo: 请求会返回总数，可根据总数来判断是否结束，如此就不会浪费一次请求

                while not page_over:
                    pois = self.get_page_poi(self.params["key"],
                        self.params["city"],
                        poly_str, 
                        typename, 
                        typecode, 
                        self.state["poi_page_cursor"]
                    )

                    for poi in pois:
                        self.__parse_poi(poi)

                        cnt+=1
                        if cnt==self.params["num_per_save"]:
                            self.flush()
                            cnt=0

                        self.state["poi_count"] += 1

                    if len(pois)<20:    #此页小于20条 => 结束了
                        page_over = True
                        self.state["poi_page_cursor"] = 0
                        break
                    else:
                        self.state["poi_page_cursor"] += 1

                self.state["typenamecodes_cursor"] += 1

            self.state["grid_cursor"] += 1

        self.flush()
        self.__success()

    def flush(self):
        out_dir = self.params["out_dir"]
        fn = out_dir.split('/')[-1]
        fp = os.path.join(out_dir, f"{fn}-{self.state['out_file_cnt']}.json")

        gdf = geopandas.GeoDataFrame(
            self.dataset["attr"],
            geometry=self.dataset["geom"]
        )
        gdf.to_file(fp, driver='GeoJSON', encoding="utf-8")

        self.state["out_file_cnt"] += 1
        self.__reset_dataset()

    def __parse_poi(self, poi):
        if self.params["city"] not in poi["cityname"]:
            return False

        # 坐标（高德地图为火星坐标）
        pnt = utils.create_point_from_str(poi["location"])
        self.dataset["geom"].append(pnt)
        # 属性
        for field in self.params["save_field"]:
            value = poi.get(field, "")
            self.dataset["attr"][field].append( str(value) )

        return True

    def __reset_dataset(self):
        self.dataset = {   #
            "attr" : {},    #attribute
            "geom" : []     #geometry
        }
        for field in self.params["save_field"]: #init attribute
            self.dataset["attr"][field] = []

    def __del__(self):
        if not self.__over:
            self.flush()
            with open(self.__cache_file, "w", encoding="utf-8") as f:
                _str = jsonpickle.encode(self)
                f.write(_str) 

    def __compute_grid(self):
        regionutil = region.AMapRegionHttp(self.__key)
        self.state["region"] = regionutil.get_region(self.__city)
        self.state["grid"] = regionutil.division_grid(self.__region, self.__col_num, self.__row_num)

    def __success(self):
        self.state["over"] = True
        print(f"[Success] A total of {self.state['total_pois']} POIs were downloaded.")
        # save paramters
        fp = os.path.join(self.__out_dir, "params.json")
        json.dump(
            self.params,
            open(fp, "w", encoding="utf-8"),
            ensure_ascii=False,
            indent=4
        )

    def get_all_type(poicode_file = "amap_poicode.xlsx"):
        """get all poi types
        :param poicode_file: (str, optional): AMap's poi code file. 
            Can be downloaded from https://lbs.amap.com/api/webservice/download
        """
        import xlrd
        myWordbook = xlrd.open_workbook(poicode_file)
        mySheets = myWordbook.sheets()
        mySheet = mySheets[2]
        # 获取列数
        nrows = mySheet.nrows
        typelist = []
        for i in range(1, nrows):
            tmp = []
            tmp.append(mySheet.cell_value(i, 4))
            tmp.append(mySheet.cell_value(i, 1))
            typelist.append(tmp)
        return typelist

    def __get_pois(self, polygon, type_name, types):
        all_page_pois = []
        while True:
            results = self.get_page_poi(polygon, type_name, types, self.state["poi_page_cursor"])
            if results == []:
                break
            all_page_pois += results
            self.state["poi_page_cursor"] += 1
        return all_page_pois

    def get_page_poi(self, key, city, polygon_str, typename, typecode, page_num):
        params = {
            "key" : key,
            'polygon' : polygon_str,  #左上右下两顶点坐标对
            "keywords" : typename,
            "types" : typecode,
            "offset" : str(20),       #官方不建议超过25，会出错。不得小于20条（小于20条代表最后一页，之后的页数将不再爬取）
            "page" : str(page_num),
            "extensions" : "all",
            "output" : "json",
            "city" : city,
            "citylimit" : True      #仅返回城市数据
        }
        # 请求报错不管，让它抛出去
        result = self.__urllib.request(AMapPOIHttp.URL, params)

        if utils.require_success(result):
            if result.get('pois')!=None:
                return result['pois']
                
        return []

if __name__ == '__main__':
    poi = AMapPOIHttp(**poi_params)
    poi.start()
    pass