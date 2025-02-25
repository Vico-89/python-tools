# Python-Tools

简单的Python小工具

### build.py

> 根据数据库表中数据生成插入SQL语句
> 
> 依赖pymsql


1. 安装依赖pymysql

   pip3 install pymysql
2. 在datasource.py中正确配置数据库连接信息
3. 配置table.properties

   table文件规则：库名|表名|具体列名判断该条数据是否存在多列以逗号隔开|是否去除id字段
   例子：vico|company_info|id|true
4. 确保sql目录存在
