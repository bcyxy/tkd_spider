import pymysql
import conf

init_db_sqls = [
    '''
    CREATE TABLE IF NOT EXISTS `spider_conf`
    (
        conf_key varchar(128),
        conf_val varchar(512)
    );
    ''',
    '''
    CREATE TABLE IF NOT EXISTS `spider_status`
    (
        status_key varchar(128),
        status_val varchar(512)
    );
    ''',
    '''
    CREATE TABLE IF NOT EXISTS `spider_data`
    (
        `domain`      varchar(256),
        `executor`    varchar(64),
        `update_time` datetime,
        `title`       varchar(1024),
        `keyword`     varchar(1024),
        `descr`       varchar(1024)
    );
    '''
]


class DBOpt():
    def __init__(self):
        self.db_h = None

    def init(self):
        'Init connect db'
        self.__connect_db()
        self.__init_tables()
        return ""

    def getTasks(self, worker_key, taskCount=20):
        sql_update = (
            "UPDATE spider_data SET `executor`='%s', update_time=NOW() "
            "WHERE `executor`=''"
            "LIMIT %u"
        ) % (worker_key, taskCount)
        sql_get = (
            "SELECT * FROM spider_data "
            "WHERE `executor`='%s'"
        ) % (worker_key)

        #self.__exe_sql(sql_update)
        #self.__exe_sql(sql_get)
        print(sql_update)
        print(sql_get)
        return []

    def __getTasks(self, worker_key, taskCount):


    def __connect_db(self):
        db_host = conf.g_conf.get_conf_str("db.host", "127.0.0.1")
        try:
            self.db_h = pymysql.connect(host=db_host,
                                        user="",
                                        password="",
                                        port=3306,
                                        database="tkd_spider",
                                        charset='utf8')
        except Exception as e:
            print(e)
            return 1
        return 0
    
    def __close_db(self):
        pass

    def __init_tables(self):
        cursor = self.db_h.cursor()
        for sql in init_db_sqls:
            cursor.execute(sql)


g_dbHdr = DBOpt()
