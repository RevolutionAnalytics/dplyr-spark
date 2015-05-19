# copyright statement
# license
#
# start thift server
# $SPARK_HOME/sbin/start-thriftserver.sh

# library(rJava)
# .jinit()
#
# library(RJDBC)
# driverclass = "org.apache.hive.jdbc.HiveDriver"
# dr = JDBC(driverclass, "/Users/antonio/Projects/Revolution/spark/assembly/target/scala-2.10/spark-assembly-1.4.0-SNAPSHOT-hadoop2.6.0.jar")
# url = "jdbc:hive2://localhost:10000"
# con = dbConnect(drv = dr, url)
#
# res = dbGetQuery(con, "CREATE TABLE IF NOT EXISTS src (key INT, value STRING)")
# res = dbGetQuery(con, "LOAD DATA LOCAL INPATH '../spark/examples/src/main/resources/kv1.txt' INTO TABLE src")
# res = dbGetQuery(con, "FROM src SELECT key, value")



src_SparkSQL =
  function(host = NULL, port = NULL, ...) {
    driverclass = "org.apache.hive.jdbc.HiveDriver"
    dr = JDBC(driverclass, "/Users/antonio/Projects/Revolution/spark/assembly/target/scala-2.10/spark-assembly-1.4.0-SNAPSHOT-hadoop2.6.0.jar")
    env = environment()
    SparkSQLConnection =
      methods::setRefClass("SparkSQLConnection", contains = "JDBCConnection", where = env)
    con =
      dbConnect(
        drv = dr,
        url = paste0("jdbc:hive2://", host, ":", port))
    con <- structure(con, class = c("SparkSQLConnection", "JDBCConnection"))
    src_sql("SparkSQL", con, info = sys.call()[-1])}

src_desc.src_SparkSQL =
  function(x) {
    paste(x$info, collapse = ":")}

src_translate_env.src_SparkSQL =
  function(x)
    sql_variant(
      base_scalar,
      sql_translator(
        .parent = base_agg,
        n = function() sql("COUNT(*)"),
        sd =  sql_prefix("STDDEV"),
        var = sql_prefix("VARIANCE")))

db_list_tables.SparkSQLConnection =
  function(con)
    dbGetQuery(con, "show tables")$tableName

db_has_table.SparkSQLConnection =
  function(con, table)
    table %in% db_list_tables(con)

db_query_fields.SparkSQLConnection =
  function(con, sql)
    names(
      dbGetQuery(
        con,
        build_sql("SELECT * FROM ", sql, " LIMIT 0", con = con)))

db_explain.SparkSQLConnection =
  function(con, sql, ...) {
    exsql <- build_sql("EXPLAIN ", sql, con = con)
    expl <- dbGetQuery(con, exsql)
    out <- capture.output(print(expl))
    paste(out, collapse = "\n")}

sql_escape_string.SparkSQLConnection =
  function(con, x)
    sql_quote(x, " ")

sql_escape_ident.SparkSQLConnection =
  function(con, x)
    sql_quote(x, " ")

tbl.src_SparkSQL =
  function(src, from, ...)
    tbl_sql("SparkSQL", src = src, from = from, ...)
