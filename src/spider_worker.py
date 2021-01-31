import conf
import hashlib
import json
import logging
import os
import random
import re
import socket
import time
from common import http_req
from db_opt import g_dbHdr
from threading import Thread

regx_domain_list = [
    re.compile(r'https*://([\w\-\.]+)'),
]


class SpiderWorker(Thread):
    def __init__(self):
        super().__init__()
        self.setDaemon(True)
        self.worker_id = self.__get_worker_id()
        logging.info("Worker id=%s" % (self.worker_id))

    def run(self):
        while not conf.g_is_exit:
            time.sleep(1)

            domain_list = self.__getTasks(
                worker_id=self.worker_id, taskCount=10)

            for domain in domain_list:
                if conf.g_is_exit:
                    break
                rst = self.__exe_task(domain)
                if rst[0] != "":
                    logging.warn("Execute task failed. %s" % rst[0])
                    continue
                logging.info("Execute task success. %s" % domain)
                self.__save_rst(rst)

    def __get_worker_id(self):
        # 先从文件中读取
        w_id = ""
        try:
            f_ih = open("../conf/worker_id", "r")
            w_id = f_ih.read()
            f_ih.close()
            w_id = w_id.strip()
        except:
            pass

        # 新生成
        if w_id == "":
            w_id = "%s#%06d" % (socket.gethostname(),
                                random.randint(0, 999999))
            logging.info("Generate worker id.")
            f_oh = open("../conf/worker_id", "w")
            f_oh.write(w_id)
            f_oh.close()

        return w_id

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
        self.__save_tkd(rst)
        self.__save_new_tasks(rst)

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

    def __save_tkd(self, rst):
        sql = (
            "UPDATE spider_data SET status='OK', update_time=NOW(), title='%s' WHERE domain='%s'"
        ) % ("TODO title", rst[1])
        err_msg = g_dbHdr.update_db(sql)
        if err_msg != "":
            logging.warn("Save tkd data failed. err_msg='%s'", err_msg)

    def __save_new_tasks(self, rst):
        rsp_text = rst[-1]
        new_domain_set = set()
        for regx in regx_domain_list:
            re_rst = regx.findall(rsp_text)
            for new_domain in re_rst:
                if new_domain in new_domain_set:
                    continue
                new_domain_set.add(new_domain)
        sql = (
            "INSERT IGNORE INTO spider_data "
            "(domain, executor, status, update_time, title, keyword, descr) VALUES "
        )
        counter = 0
        for domain in new_domain_set:
            if counter > 0:
                sql += ","
            sql += "('%s', '', '%s', NOW(), '', '', '')" % (domain, "INIT")
            counter += 1
        err_msg = g_dbHdr.update_db(sql)
        if err_msg != "":
            logging.warn("Save new tasks failed. err_msg='%s'", err_msg)

    def __getTasks(self, worker_id, taskCount=20):
        sql_update = (
            "UPDATE spider_data SET `executor`='%s', update_time=NOW() "
            "WHERE `executor`=''"
            "LIMIT %u"
        ) % (worker_id, taskCount)
        sql_get = (
            "SELECT domain FROM spider_data "
            "WHERE `executor`='%s' AND status='INIT'"
        ) % (worker_id)

        err_msg = g_dbHdr.update_db(sql_update)
        if err_msg != "":
            return []
        db_rows, err_msg = g_dbHdr.select_db(sql_get)
        if err_msg != "":
            return []
        tasks = []
        for db_row in db_rows:
            tasks.append(db_row[0])
        return tasks

    def __save_to_db(self, rst):
        pass
