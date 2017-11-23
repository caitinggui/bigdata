# cofing:utf-8

# type(json_data): pyspark.sql.dataframe.DataFrame
json_data = sqlContext.read.json('input/xbb')
# or json_data.registerTempTable('inverted_table')
sqlContext.registerDataFrameAsTable(json_data, 'inverted_table')
# 缓存整个表, 操作是lazy的，需要action触发,取消为uncacheTable
sqlContext.cacheTable('inverted_table')
df = sqlContext.sql("SELECT * FROM inverted_table where key='321.281401'")
results = df.toJSON()



results = df.collect()
result = results[0]
result.asDict()
