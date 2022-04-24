from amap_api import defines
from amap_api import region
from amap_api import utils
from common import urllibagent

import os
import json
import jsonpickle

import geopandas

poi_params = {
    "key" : defines.AMAP_KEY,

    #输出路径（不要后缀名，程序会自动加json）
    "out_dir": "上海_gcj02",
    
    # 城市
    "city" : "上海",
    
    # POI类型（可查看官方给的文档）
    "typenamecodes" : [
       ["地名地址信息", "190000"]
       ,["餐饮服务", "050000"]
       ,["道路附属设施", "180000"]
       ,["公司企业", "170000"]
       ,["购物服务", "060000"]
       ,["交通设施服务", "150000"]
       ,["金融保险服务", "160000"]
       ,["科教文化服务", "140000"]
       ,["商务住宅", "120000"]
       ,["医疗保健服务", "090000"]
       ,["政府机构及社会团体", "130000"]
    ],
    
    #保存的字段。可选，默认为id、name
    "save_field" : [
        "timestamp",    #"2022-04-07 18:55:08"
        "id",           #"B00155P630"
        "name",         #"浦东新区"
        "type",         #"地名地址信息;普通地名;区县级地名"
        "typecode",     #"190105"
        "adname",       #"浦东新区"
        "address",      #"浦东新区"
        "adcode",       #"310115"
        "pname",        #"上海市"
        "citycode",     #"021"
        "cityname",     #"上海市"
        "pcode",        #"310000"
        "gridcode",     #"4621646311"
        "shopinfo",     #"2"
        "navi_poiid",   #"H51F010013_99999345"
        "entr_location"    #"121.544698,31.222589"。"location"="121.544346,31.221461"
    ],

    # 每一次存储到文件的数据量
    "num_per_save" : 200,    #10页保存一次

    # 将城市范围的矩形分成row_num行、col_num列的小矩阵
    #如果城市范围较大，划分的要尽可能大，不然可能会漏下载（因为一次参数最多有1000条）
    "num_row" : 5,
    "num_col" : 5
}

class AMapPOIAPI(object):
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
        , key 
        , city
        , typenamecodes = None
        , save_field =  ["id", "name"]
        , out_dir = "poi"
        , num_row = 4
        , num_col = 4
        , num_per_save = 500
        , load_cache = True
        ):
        
        if load_cache and os.path.exists(self.__cache_file): #State cache, whether rollback
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
            self.params["key"] = key
            self.params["city"] = city

            if typenamecodes is None or len(typenamecodes)==0:
                self.params["typenamecodes"] = self.get_all_type()
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

        # save paramters
        fp = os.path.join(self.params["out_dir"], "params.json")
        json.dump(
            self.params,
            open(fp, "w", encoding="utf-8"),
            ensure_ascii=False,
            indent=4
        )
        
        super(AMapPOIAPI, self).__init__()

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
            print("正在第{}个方格区搜索".format(self.state['grid_cursor'] + 1))

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
                    print("\t一次请求，获得{}个POI点，正在解析".format(len(pois)))

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

            self.state["typenamecodes_cursor"] = 0
            self.state["grid_cursor"] += 1

        self.flush()
        self.__success()

    def flush(self):
        if len(self.dataset["geom"])==0:
            return

        out_dir = self.params["out_dir"]

        fn = "{}_{}_{}.json".format(
            self.state['out_file_cnt'],
            self.params["city"],
            self.state["grid_cursor"]
        )
        fp = os.path.join(out_dir, fn)

        gdf = geopandas.GeoDataFrame(
            self.dataset["attr"],
            geometry=self.dataset["geom"]
        )
        gdf.to_file(fp, driver='GeoJSON', encoding="utf-8")
        print("\t\t保存结果".format(fp))

        self.state["out_file_cnt"] += 1
        self.__reset_dataset()

    def __parse_poi(self, poi):
        # if self.params["city"] not in poi["cityname"]:    #请求时，已经设置仅返回该城市的数据
        #     return False

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
        self.flush()

        if not self.state["over"]:
            with open(self.__cache_file, "w", encoding="utf-8") as f:
                _str = jsonpickle.encode(self)
                f.write(_str)
                print("任务未结束，但已存档{}".format(self.__cache_file))

    def __compute_grid(self):
        regionutil = region.AMapRegionAPI(self.params["key"])
        self.state["rect"] = regionutil.get_rect(self.params["city"])
        self.state["grid"] = regionutil.division_grid(self.state["rect"], self.params["col_num"], self.params["row_num"])

    def __success(self):
        self.state["over"] = True
        print(f"[Success] A total of {self.state['poi_count']} POIs were downloaded.")

        fp = os.path.join(self.params["out_dir"], "task.json")
        with open(fp, "w", encoding="utf-8") as f:
            _str = jsonpickle.encode(self)
            f.write(_str)

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
        result = self.__urllib.request(AMapPOIAPI.URL, params)

        if utils.require_success(result):
            if result.get('pois')!=None:
                return result['pois']
                
        return []

if __name__ == '__main__':
    try:
        poi = AMapPOIAPI(**poi_params)
        poi.start()
    except Exception as e:
        print(f"Error: {e}")
    pass

# if __name__ == '__main__':
#     from threading import Thread
#     poi = AMapPOIAPI(**poi_params)
#     t = Thread(target=lambda: poi.start())
#     t.start()

#     from common import getchar
#     getchar.getchar("键入任意字符结束下载……")
#     pass