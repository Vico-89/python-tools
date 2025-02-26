import pymysql


def init():
    conn = pymysql.connect(host='127.0.0.1', port=3306, password='123456', user='root', db='demo',
                           charset='utf8')
    return conn


def get_table_column(table_name, table_schema):
    conn = init()
    cur = conn.cursor()

    select_table_cmd = 'SELECT COLUMN_NAME,ORDINAL_POSITION FROM information_schema.COLUMNS ' \
                       f'WHERE table_name = \'{table_name}\' AND table_schema = \'{table_schema}\''

    cur.execute(select_table_cmd)
    data = cur.fetchall()
    print(f'查询到{table_name}表字段：')
    for column in data:
        print(column)
    cur.close()
    conn.close()
    return sorted(data, key=lambda t: t[1])


def get_data(table_name, table_schema):
    conn = init()
    cur = conn.cursor()
    select_data_cmd = f'SELECT * FROM `{table_schema}`.`{table_name}`'

    cur.execute(select_data_cmd)
    data_list = cur.fetchall()
    cur.close()
    conn.close()
    return data_list
