from conf import g_conf


def main():
    # 启动配置模块线程
    err_msg = g_conf.start()
    if err_msg != "":
        print(err_msg)
        return
    
    # 启动workers


if __name__ == "__main__":
    main()
