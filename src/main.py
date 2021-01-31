import conf
import logging
import signal
from db_opt import g_dbHdr
from spider_worker import SpiderWorker


def exit(signum, frame):
    conf.g_is_exit = True


signal.signal(signal.SIGINT, exit)
signal.signal(signal.SIGTERM, exit)


def main():
    # 初始化配置模块
    err_msg = conf.g_conf.init()
    if err_msg != "":
        logging.error("Initial config failed. msg=%s" % err_msg)
        return

    # 初始化数据库
    err_msg = g_dbHdr.init()
    if err_msg != "":
        logging.error("Initial db failed. msg=%s" % err_msg)
        return

    # 启动配置模块进程
    conf.g_conf.start()

    # 启动worker
    workersMg = SpiderWorker()
    workersMg.run()


if __name__ == "__main__":
    main()
