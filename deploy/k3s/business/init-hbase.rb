# HBase shell script. Numeric cells use the binary encoding expected by Flink.
['dim_user_info', 'dim_geo_area', 'dim_page_info'].each do |table|
  create table, 'f' unless exists(table)
end
bytes = org.apache.hadoop.hbase.util.Bytes
put 'dim_user_info', 'accept-user', 'f:id', bytes.toBytes(10001.to_java(:long))
put 'dim_user_info', 'accept-user', 'f:phone', '13800000000'
put 'dim_user_info', 'accept-user', 'f:status', bytes.toBytes(1.to_java(:int))
put 'dim_user_info', 'accept-user', 'f:create_time', bytes.toBytes(1704067200000.to_java(:long))
put 'dim_user_info', 'accept-user', 'f:gender', bytes.toBytes(1.to_java(:int))
put 'dim_user_info', 'accept-user', 'f:birthday', bytes.toBytes(7305.to_java(:int))
put 'dim_user_info', 'accept-user', 'f:province', 'Beijing'
put 'dim_user_info', 'accept-user', 'f:city', 'Beijing'
put 'dim_user_info', 'accept-user', 'f:job', 'tester'
put 'dim_user_info', 'accept-user', 'f:source_type', bytes.toBytes(1.to_java(:int))
# Reverse of GeoHash(39.9, 116.4, precision=5): wx4fb -> bf4xw.
put 'dim_geo_area', 'bf4xw', 'f:province', 'Beijing'
put 'dim_geo_area', 'bf4xw', 'f:city', 'Beijing'
put 'dim_geo_area', 'bf4xw', 'f:region', 'Haidian'
put 'dim_page_info', '/mall/item/', 'f:pt', 'product'
put 'dim_page_info', '/mall/item/', 'f:sv', 'mall'
get 'dim_user_info', 'accept-user'
get 'dim_geo_area', 'bf4xw'
get 'dim_page_info', '/mall/item/'
