"""
@Author  : geodoer
@Time    :  2022/4/12 22.16
@Email   : geodoer@163.com
@Func    : Urllib的代理类
@Desc    : 
    1. 可添加请求限制器，用于限流
    2. 提供状态保存与加载
"""
import json

import urllib
import urllib.request

from common.limiter import RequestLimitsRule, RequestLimiter

class UrllibAgent(RequestLimiter):
    """Urllib代理层
    不支持并发，本身带Key请求，速度也提不起来
    """
    def __init__(self, name, *many_request_limits) -> None:
        super().__init__(name, many_request_limits)
        
    
    def request(self, url, params):
        param_str = urllib.parse.urlencode(params)
        req_url = f'{url}?{param_str}'

        self.wait()   #等待一次份额

        with urllib.request.urlopen(req_url) as f:
            data = f.read()
            data = data.decode('utf-8')
            obj = json.loads(data)
            return obj

if __name__ == '__main__':
    urllib_agent = UrllibAgent("test"
        ,RequestLimitsRule(50, 1)
        ,RequestLimitsRule(30000, 24 * 60 * 60)
    )

    for i in range(0, 10000):
        if i%100==0:
            print(f"已请求{i}")
        urllib_agent.wait()

    pass