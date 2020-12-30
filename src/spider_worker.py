import json
import time
from common import http_req

class SpiderWorker():
    def __init__(self):
        pass

    def start(self):
        self.__run()  # TODO 多线程

    def __run(self):
        while True:
            task = self.__get_task()
            rst = self.__exe_task(task)
            self.__save_rst(rst)
            break  # TODO
            time.sleep(1)

    def __get_task(self):
        '从数据库中获取任务'
        return "segmentfault.com"  # TODO

    def __exe_task(self, task):
        '执行任务'
        err_msg, rsp_code, rsp_headers, rsp_text = http_req(
            url="http://%s" % task,
            methon="GET",
            headers={
                "User-Agent": "Mozilla/4.0"
            },
            datas={
                "aaa": "bbb"
            }
        )
        if err_msg != "":
            print(err_msg)
            return
        return err_msg, rsp_code, rsp_headers, rsp_text
    
    def __save_rst(self, rst):
        '存储结果'
        print(rst)


class WorkersManager():
    def __init__(self, worker_count):
        self.__workers_list = []
        for _ in range(0, worker_count):
            worker = SpiderWorker()
            worker.start()
            self.__workers_list.append(worker)

    def start_workers(self):
        pass
