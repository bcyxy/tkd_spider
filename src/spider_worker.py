import conf
import hashlib
import json
import logging
import os
import time
from common import http_req
from db_opt import g_dbHdr
from threading import Thread


class SpiderWorker(Thread):
    def __init__(self):
        super().__init__()
        self.setDaemon(True)
        self.worker_id = 1

    def run(self):
        while True:
            time.sleep(1)

            worker_key = "%s.%s" % (conf.g_instance_key, self.worker_id)
            domain_list = g_dbHdr.getTasks(worker_key=worker_key, taskCount=10)

            for domain in domain_list:
                rst = self.__exe_task(domain)
                if rst[0] != "":
                    logging.warn("Execute task failed. %s" % rst[0])
                    continue
                self.__save_rst(rst)

    def __exe_task(self, task):
        '执行任务'
        err_msg, rsp_code, rsp_headers, rsp_text = http_req(
            url="http://%s" % task,
            methon="GET",
            headers={
                "User-Agent": "Mozilla/4.0"
            },
            datas={}
        )

        return err_msg, task, rsp_code, rsp_headers, rsp_text

    def __save_rst(self, rst):
        '存储结果'
        self.__save_orig_text(rst)
        self.__save_orig_text(rst)

    def __save_orig_text(self, rst):
        domain = rst[1]
        domain_md5 = hashlib.md5(domain.encode('utf-8')).hexdigest()
        f_dir = "../data/%s/%s/%s" % (domain_md5[0],
                                      domain_md5[1],
                                      domain_md5[2])
        f_name = "%d_%s" % (time.time(), domain)
        os.system("mkdir -p %s" % f_dir)
        f_path = os.path.join(f_dir, f_name)
        f_oh = open(f_path, "w")
        f_oh.write(rst[-1])
        f_oh.close()
        logging.debug("Save original data success. f_path=%s" % f_path)

    def __save_to_db(self, rst):
        pass


class WorkersManager():
    def __init__(self, worker_count):
        self.__workers_list = []
        for _ in range(0, worker_count):
            self.__workers_list.append(SpiderWorker())

    def start_workers(self):
        for worker in self.__workers_list:
            worker.start()
            logging.info("Start worker. thread_name=%s." % worker.getName())

    def join(self):
        for worker in self.__workers_list:
            worker.join()
