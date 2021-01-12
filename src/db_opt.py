import pymysql
from conf import g_conf

sql_template_create_tb = '''
CREATE TABLE IF NOT EXISTS `%s`
(
    id int,
    name int
);
'''


class DBOpt():
    def __init__(self):
        pass

    def init(self):
        'Init connect db'
        self.__connect_db()
        self.__init_tables()
        return ""

    def __connect_db(self):
        self.db_h = pymysql.connect(host=g_conf.get_conf_str("db.host", "127.0.0.1"),
                                    user="",
                                    password="",
                                    port=3306,
                                    database="tkd_spider",
                                    charset='utf8')
        cursor = self.db_h.cursor()
        cursor.execute(sql_template_create_tb % "aaa")

    def __init_tables(self):
        sql_create_tb = sql_template_create_tb % "aaa"
        print(sql_create_tb)


g_dbHdr = DBOpt()
