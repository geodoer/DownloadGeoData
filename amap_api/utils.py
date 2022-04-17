
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