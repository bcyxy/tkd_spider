
sql_template_create_tb = '''
CREATE IF NOT EXISTS TABLES `%s`
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
        self.__init_tables()
        self.__connect_db()

    def __init_tables(self):
        sql_create_tb = sql_template_create_tb % "aaa"
        print(sql_create_tb)

    def __connect_db(self):
        pass
