import datetime

from datasource import datasource
from distutils.util import strtobool

# 配置文件名，用于配置表清单
_FILE_NAME = 'table.properties'
_INSERT_PREFIX = 'INSERT INTO '
_VALUE = ') VALUE '
_NOW = 'now()'
_SELECT_PREFIX = 'select '
_CURRENT_TIMESTAMP = 'CURRENT_TIMESTAMP()'

"""
1. 安装依赖pymysql
pip3 install pymysql
2. 在datasource.py中正确配置数据库连接信息
3. 配置table.properties
table文件规则：库名|表名|具体列名判断该条数据是否存在多列以逗号隔开|是否去除id字段
例子：vico|company_info|id|true
4. 确保sql目录存在
"""


def load_file_build():
    for line in open(_FILE_NAME):
        if line.startswith('#'):
            continue
        line = line.strip('\n')
        table_column = line.split('|')
        build(table_column[1], table_column[0], table_column[2].split(','), strtobool(table_column[3]))


def build(table_name, table_schema, column_name, is_delete_id):
    """
        根据数据库中数据生成可重复执行sql
    :param table_name: 表名
    :param table_schema: 库名
    :param column_name: 哪列判断该条数据是否存在
    :param is_delete_id: 是否去出id列
    """
    # MySQL
    file = create_sql_file(table_name, 'my')
    table = datasource.get_table_column(table_name, table_schema)
    prefix = build_prefix(table, table_name, is_delete_id, 1)
    data_list = datasource.get_data(table_name, table_schema)
    for data in data_list:
        insert_cmd = build_insert(prefix, data, is_delete_id, 1)
        script = build_mysql_script(table, insert_cmd, table_name, data, column_name)
        file.write(script)
    file.close()

    # 达梦
    file = create_sql_file(table_name, 'dm')
    table = datasource.get_table_column(table_name, table_schema)
    prefix = build_prefix(table, table_name, is_delete_id, 0)
    data_list = datasource.get_data(table_name, table_schema)
    for data in data_list:
        insert_cmd = build_insert(prefix, data, is_delete_id, 0)
        script = build_dm_script(table, insert_cmd, table_name, data, column_name, is_delete_id)
        file.write(script)
    file.close()

    # Oracle
    file = create_sql_file(table_name, 'oracle')
    table = datasource.get_table_column(table_name, table_schema)
    prefix = build_prefix(table, table_name, is_delete_id, 0)
    data_list = datasource.get_data(table_name, table_schema)
    for data in data_list:
        insert_cmd = build_insert(prefix, data, is_delete_id, 0)
        script = build_oracle_script(table, insert_cmd, table_name, data, column_name)
        file.write(script)
    file.close()


# -- MySQL插入语句示例
# set @hs_sql = 'select 1 from dual;';
#
# select 'INSERT INTO ft_agmt_config (id, prod_kind, prod_type, prod_inner_code, agree_type, agree_name, exec_rule, filling_content, is_show, create_datetime, update_datetime) VALUE (51, ''3'', null, null, ''33'', ''代销风险揭示书'', ''{"function_id":"29004","prod_code":"007624","prodta_no":"04","busi_data":{"protocol_type":"consignment"}}'', null, 1, now(), now());'
# into @hs_sql
# from dual
# where (select count(*) FROM ft_agmt_config where agree_type = '33' and prod_kind = '3' ) = 0;
# PREPARE hs_stmt FROM @hs_sql;
# EXECUTE hs_stmt;
# DEALLOCATE PREPARE hs_stmt;
# commit;


def build_mysql_script(table, insert_cmd, table_name, data, column_name):
    script = 'set @hs_sql = \'select 1 from dual;\';\n\n'
    script += _SELECT_PREFIX + '\'' + insert_cmd + '\'\ninto @hs_sql \nfrom dual \nwhere ('
    script += f'select count(*) FROM {table_name} where '
    script += build_count_where_cmd(table, data, column_name)
    script += ') = 0;\n'
    script += 'PREPARE hs_stmt FROM @hs_sql;\n' + 'EXECUTE hs_stmt;\n' + 'DEALLOCATE PREPARE hs_stmt;\n' + 'commit;\n'
    script += '\n'
    return script


# -- 达梦插入语句示例
# declare v_rowcount number(5);
# begin
# 	select count(*) into v_rowcount from dual where exists (select 1 FROM ft_agmt_config where agree_type = '33' and prod_kind = '3');
# 	if v_rowcount = 0 then
# 		SET IDENTITY_INSERT ft_agmt_config ON;
#         INSERT INTO ft_agmt_config (id, prod_kind, prod_type, prod_inner_code, agree_type, agree_name, exec_rule, filling_content, is_show, create_datetime, update_datetime) VALUES (51, '3', null, null, '33', '代销风险揭示书', '{"function_id":"29004","prod_code":"007624","prodta_no":"04","busi_data":{"protocol_type":"consignment"}}', null, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
#     	SET IDENTITY_INSERT ft_agmt_config OFF;
#     end if;
# 	commit;
# end;
# /


def build_dm_script(table, insert_cmd, table_name, data, column_name, is_delete_id):
    script = 'declare v_rowcount number(5);\n'
    script += 'begin\n'
    script += f'    select count(*) into v_rowcount from dual where exists (select 1 FROM {table_name} where '
    script += build_count_where_cmd(table, data, column_name)
    script += ');\n'
    script += '	if v_rowcount = 0 then\n'
    if not is_delete_id:
        script += f'		SET IDENTITY_INSERT {table_name} ON;\n'

    script += '		' + insert_cmd + '\n'
    if not is_delete_id:
        script += f'		SET IDENTITY_INSERT {table_name} OFF;\n'

    script += '    end if;\n'
    script += '    commit;\n'
    script += 'end;\n'
    script += '/\n\n'
    return script


def build_oracle_script(table, insert_cmd, table_name, data, column_name):
    script = 'declare v_rowcount number(5);\n'
    script += 'begin\n'
    script += f'    select count(*) into v_rowcount from dual where exists (select 1 FROM {table_name} where '
    script += build_count_where_cmd(table, data, column_name)
    script += ');\n'
    script += '	if v_rowcount = 0 then\n'
    script += '		' + insert_cmd + '\n'
    script += '    end if;\n'
    script += '    commit;\n'
    script += 'end;\n'
    script += '/\n\n'
    return script


# 生成判断该数据是否存在sql语句where条件
def build_count_where_cmd(table, data, column_name):
    script = ''
    for name in column_name:
        data_index = get_value_index(name, table)
        value = data[data_index]
        if isinstance(value, int):
            script += name + ' = ' + str(value) + ' and '
        elif isinstance(value, str):
            script += name + ' = \'' + value + '\'' + ' and '
    # 去除最后一个and
    script = script[0: len(script) - 4]
    return script


# 生成insert语句
def build_insert(prefix, data, is_delete_id, mysql):
    sql_cmd = prefix + '('
    for idx, column_data in enumerate(data):
        if idx == 0 and is_delete_id:
            continue
        if isinstance(column_data, int):
            sql_cmd = '%s%s' % (sql_cmd, column_data)
        elif isinstance(column_data, str):
            sql_cmd += '\'\'' + column_data + '\'\'' if mysql else '\'' + column_data + '\''
        elif isinstance(column_data, datetime.datetime):
            sql_cmd += _NOW if mysql else _CURRENT_TIMESTAMP
        elif column_data is None:
            sql_cmd += 'null'
        sql_cmd += ', '
    # 截取最后一个逗号
    sql_cmd = sql_cmd[0: len(sql_cmd) - 2]
    sql_cmd += ');'
    print(sql_cmd)
    return sql_cmd


def get_value_index(column_name, table):
    for idx, column in enumerate(table):
        if column_name == column[0]:
            return idx


def build_prefix(table, table_name, is_delete_id, mysql):
    prefix = _INSERT_PREFIX + table_name + ' ('

    for column in table:
        # 是否删除id列
        if is_delete_id & (column[0] == 'id'):
            continue
        prefix = prefix + column[0] + ', '
    prefix = prefix[:-2]
    prefix += _VALUE if mysql else ') VALUES '

    print('SQL前缀：' + prefix)
    return prefix


def create_sql_file(file_name, suffix):
    file = open(f'./sql/{suffix}/{file_name}_insert_{suffix}.sql', 'w', encoding='utf-8')
    return file


if __name__ == '__main__':
    # build('fm_dict_item', 'hspf', ['dict_entry_code', 'dict_item_code'])
    load_file_build()
