import logging
import pymysql
import conf


class DBOpt():
    def __init__(self):
        self.db_h = None

    def init(self):
        'Init db'
        # 连接数据库
        err_code, err_msg = self.__connect_db()
        if err_code != 0:
            return err_msg

        # 初始化数据库
        self.__init_tables()
        return ""

    def select_db(self, sql):
        # 执行SQL
        db_rows, err_code, err_msg = self.__select_db(sql)
        if err_code == 2013:
            pass
        else:
            return db_rows, err_msg

        # 重连数据库
        err_code, err_msg = self.__connect_db()
        if err_code != 0:
            return (), err_msg

        # 再次执行SQL
        db_rows, err_code, err_msg = self.__select_db(sql)
        return db_rows, err_msg

    def update_db(self, sql):
        # 执行SQL
        err_code, err_msg = self.__update_db(sql)
        if err_code == 2013:
            pass
        else:
            return err_msg

        # 重连数据库
        err_code, err_msg = self.__connect_db()
        if err_code != 0:
            return err_msg

        # 再次执行SQL
        err_code, err_msg = self.__update_db(sql)
        return err_msg

    def __connect_db(self):
        db_host = conf.g_conf.get_conf_str("db.host", "127.0.0.1")
        db_port = conf.g_conf.get_conf_int("db.port", 3306)
        db_name = conf.g_conf.get_conf_str("db.db_name", "tkd_spider")
        db_user = conf.g_conf.get_conf_str("db.user", "")
        db_passwd = conf.g_conf.get_conf_str("db.password", "")
        try:
            self.db_h = pymysql.connect(host=db_host,
                                        user=db_user,
                                        password=db_passwd,
                                        port=db_port,
                                        database=db_name,
                                        charset='utf8')
        except Exception as e:
            logging.warn("Connect db '%s:%u/%s' failed. msg=%s"
                         % (db_host, db_port, db_name, e))
            if len(e.args) == 2:
                return e.args[0], e.args[1]
            return -1, str(e)
        return 0, ""

    def __select_db(self, sql):
        cursor = self.db_h.cursor()
        try:
            cursor.execute(sql)
            return cursor.fetchall(), 0, ""
        except Exception as e:
            if len(e.args) == 2:
                return (), e.args[0], e.args[1]
            else:
                return (), -1, e

    def __update_db(self, sql):
        cursor = self.db_h.cursor()
        try:
            cursor.execute(sql)
            self.db_h.commit()  # TODO 连接级别
            return 0, ""
        except Exception as e:
            if len(e.args) == 2:
                return e.args[0], e.args[1]
            else:
                return -1, e

    def __init_tables(self):
        init_db_sqls = [
            '''
            CREATE TABLE IF NOT EXISTS `spider_conf`
            (
                conf_key varchar(128),
                conf_val varchar(512),
                UNIQUE KEY `idx_conf_key` (`conf_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''',
            '''
            CREATE TABLE IF NOT EXISTS `spider_status`
            (
                status_key varchar(128),
                status_val varchar(512),
                UNIQUE KEY `idx_status_key` (`status_key`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''',
            '''
            CREATE TABLE `spider_data` (
                `domain`      varchar(256)  DEFAULT NULL,
                `executor`    varchar(64)   DEFAULT NULL,
                `status`      varchar(16)   DEFAULT NULL,
                `update_time` datetime      DEFAULT NULL,
                `title`       varchar(1024) DEFAULT NULL,
                `keyword`     varchar(1024) DEFAULT NULL,
                `descr`       varchar(1024) DEFAULT NULL,
                UNIQUE KEY `idx_domain` (`domain`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''',
            '''
            CREATE TABLE `domain_conf` (
                `type`       varchar(32)  DEFAULT NULL,
                `top_domain` varchar(32)  DEFAULT NULL,
                `descr`      varchar(128) DEFAULT NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8
            ''',
        ]
        for sql in init_db_sqls:
            self.update_db(sql)


g_dbHdr = DBOpt()
