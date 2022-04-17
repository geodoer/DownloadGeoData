import time
import os
import jsonpickle

class RequestLimitsRule:
    """限额规则
    每time_range，只能处理limit次
    :param limit:       限额，单位：次
    :param time_range:  时间范围，单位：秒
    """
    def __init__(self, limit=400, time_range=60) -> None:
        self.limit = limit
        self.time_range = time_range

        self.reset()

    def wait(self):
        """等待一次份额
        """
        if self.start_time:   #已经开始计时
            # 没有超流量
            if self.count < self.limit:
                self.count += 1
                return

            # 超流量
            end_time = self.start_time + self.time_range
            cur_time = time.time()

            #如果没有到结束时间 => 睡眠到结束时间
            if cur_time < end_time:
                wait_time = end_time - cur_time
                print("当前时间{}; 已超额（{}/{}）; 将等待{}s（{} -> {}）".format(
                    self.__time_to_str(cur_time),
                    self.count,
                    self.limit,
                    wait_time,
                    self.__time_to_str(self.start_time),
                    self.__time_to_str(end_time)
                ))
                time.sleep(wait_time)
            
            #已经到了结束时间
            self.__start()
            return
        else: #第一次请求
            self.__start()
            return

    def reset(self):
        """重置
        """
        self.count = 0
        self.start_time = None

    def is_over(self):
        """此轮限额已结束
        """
        end_time = self.start_time + self.time_range
        cur_time = time.time()
        return cur_time > end_time

    def __time_to_str(self, ct):
        local_time = time.localtime(ct)
        data_head = time.strftime("%Y-%m-%d %H:%M:%S", local_time)
        data_secs = (ct - int(ct)) * 1000
        time_stamp = "%s.%03d" % (data_head, data_secs)
        return time_stamp
    
    def __lt__(self, rhs):
        """<运算符
        """
        return self.time_range < rhs.time_range

    def __start(self):
        """计时开始
        """
        self.count = 1
        self.start_time = time.time()

class RequestLimiter:
    """限额器，内部支持多个限额规则
    :param many_request_limits: 一个RequestLimitsRule 或 多个RequestLimitsRule
    """
    def __init__(self, name="default", *many_request_limits):
        self.name = name

        #init rules
        self.rules = []
        for rule in many_request_limits:
            if type(rule) != RequestLimitsRule:
                raise Exception("Please pass in one or more RequestLimitsRules.")
        self.rules += many_request_limits
        self.rules.sort(reverse=True) #降序排序，先考虑时间间隔大的限额规则

        #存在缓存，加载状态
        if os.path.exists(self.__cache_file):
            with open(self.__cache_file, encoding="utf-8") as f:
                self = jsonpickle.decode(f.read())

                #加载状态之后，检查是否都over
                if self.is_over():
                    self.reset()

                return

    @property
    def __cache_file(self)->str:
        return f"cache-{self.__class__.__name__}-{self.name}.json"

    def __del__(self):
        if self.is_over(): #结束 -> 不保存
            if os.path.exists(self.__cache_file): #存在 -> 删除
                os.remove(self.__cache_file)
            return

        #保存状态
        _str = jsonpickle.encode(self)
        with open(self.__cache_file, "w", encoding="utf-8") as f:
            f.write(_str)

    def reset(self):
        """重置所有限额
        """
        for rule in self.rules:
            rule.reset()

    def wait(self):
        """等待下一次限额
        """
        for rule in self.rules:
            rule.wait()

    def is_over(self):
        """此论限额是否结束
        """
        flag = True
        for rule in self.rules:
            flag &= rule.is_over()
        return flag

if __name__ == '__main__':
    limiter = RequestLimiter("test",
        RequestLimitsRule(300, 3),  #1秒限额50
        RequestLimitsRule(400, 5)   #10秒限额350
    )
    # limiter = RequestLimiter("test"
    #     ,RequestLimitsRule(50, 1)        #1秒限额50
    #     ,RequestLimitsRule(30000, 24 * 60 * 60) #一天限额30000
    # )

    for i in range(0, 1000):
        if i%100==0:
            print(f"此次已请求{i}")
        limiter.wait()