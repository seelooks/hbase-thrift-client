# hbase 查询web服务

## 这是一个用thrift编写的hbase 查询web服务。主要的查询功能有

### 1、查询hbase中有哪些表

- 查询地址：http://10.213.245.158:8805/all/table
- 备注：无参数

### 2、查询表中存在哪些列

- 查询地址：http://10.213.245.158:8805/cell/{tableName}
- 示例：http://10.213.245.158:8805/cell/crecord

### 3、表数据查询
- 查询地址： http://10.213.245.158:8805/data/{tableName}/{size}
- 示例：http://10.213.245.158:8805/data/crecord/100

