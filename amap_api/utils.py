from shapely.geometry import point

def parse_point_str(point_str):
    tmp = point_str.split(',')
    return float(tmp[0]), float(tmp[1])

def create_point_from_str(location_str):
    tmp = parse_point_str(location_str)
    return point.Point(tmp[0], tmp[1])
    
def parse_polyline_str(polyline_str):
    """解析AMap HTTP Polyline字符串
    """
    try:
        lines_str = polyline_str.split('|')
        lines = []
        for line_str in lines_str:
            line = []
            lnglats = line_str.split(';')
            for lnglat in lnglats:
                lng,lat = lnglat.split(',')
                line.append(
                    ( float(lng), float(lat)  )
                )
            lines.append(line )
        return lines
    except:
        return []

#
# 网络请求
# 
def require_success(obj):
    status = obj.get("status", '0')

    if status is '1':
        return True
    
    return False