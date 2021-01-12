import configparser
import logging
import os
import time


log_dir = "../log"
os.system("mkdir -p %s" % log_dir)
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s|%(levelname)s|%(filename)s:%(lineno)d|%(message)s",
    datefmt="%Y%m%d_%H%M%S",
    filename="%s/spider.log" % log_dir,
    filemode="a"
)


class Config():
    def __init__(self):
        self.conf_path = "../conf/spider.conf"
        self.conf_hd = configparser.ConfigParser()
        self.conf_cache = {}
    
    def init(self):
        self.__load_local_conf()
        return ""

    def start(self):
        '启动配置更新线程'
        # 加载本地配置文件，加载一次
        err_msg = self.__load_local_conf()
        if err_msg != "":
            return err_msg

        # 加载数据库配置，周期更新
        # thread self.__load_db_conf()
        return ""

    def get_conf_str(self, conf_key, default_val):
        #TODO lock
        return self.conf_cache.get(conf_key, default_val)

    def get_conf_int(self, conf_key, default_val):
        # TODO
        return default_val

    def __load_local_conf(self):
        try:
            self.conf_hd.read(self.conf_path)
        except Exception as e:
            return "Read local config failed. err=%s" % e
        sections = self.conf_hd.sections()
        for st_name in sections:
            for conf_sub_key, conf_val in self.conf_hd.items(st_name):
                conf_key = "%s.%s" % (st_name, conf_sub_key)
                self.conf_cache[conf_key] = conf_val
                logging.debug("Load local conf: %s=%s" % (conf_key, conf_val))
        return ""

    def __load_db_conf(self):
        # TODO
        return 0


g_conf = Config()
