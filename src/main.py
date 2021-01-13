import logging
from conf import g_conf
from db_opt import g_dbHdr
from spider_worker import WorkersManager


def main():
    # 初始化配置模块
    err_msg = g_conf.init()
    if err_msg != "":
        logging.error("Initial config failed. msg=%s" % err_msg)
        return

    # 初始化数据库
    err_msg = g_dbHdr.init()
    if err_msg != "":
        logging.error("Initial db failed. msg=%s" % err_msg)
        return

    # 启动配置模块进程
    g_conf.start()

    # 启动workers
    workersMg = WorkersManager(worker_count=1)
    workersMg.start_workers()
    workersMg.join()


if __name__ == "__main__":
    main()
