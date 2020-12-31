from conf import g_conf
from spider_worker import WorkersManager


def main():
    # 启动配置模块线程
    err_msg = g_conf.start()
    if err_msg != "":
        print(err_msg)
        return

    # 启动workers
    workersMg = WorkersManager(worker_count=1)
    workersMg.start_workers()
    workersMg.join()


if __name__ == "__main__":
    main()
