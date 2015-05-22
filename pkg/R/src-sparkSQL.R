# Copyright 2015 Revolution Analytics
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


setClass("SparkSQLConnection", contains = "JDBCConnection")

src_SparkSQL =
  function(host = "localhost", port = 10000) {
    driverclass = "org.apache.hive.jdbc.HiveDriver"
    dr = JDBC(driverclass, Sys.getenv("HADOOP_JAR"))
    con =
      dbConnect(
        drv = dr,
        url = paste0("jdbc:hive2://", host, ":", port))
    con = new("SparkSQLConnection", con)
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
        build_sql("SELECT * FROM ", sql_subquery(con, sql, "master"), " LIMIT 0", con = con)))

db_explain.SparkSQLConnection =
  function(con, sql, ...) {
    exsql <- build_sql("EXPLAIN ", sql, con = con)
    expl <- dbGetQuery(con, exsql)
    out <- capture.output(print(expl))
    paste(out, collapse = "\n")}

db_begin.SparkSQLConnection =
  function(con, ...) TRUE

db_commit.SparkSQLConnection =
  function(con, ...) TRUE

db_rollback.SparkSQLConnection =
  function(con, ...) TRUE

db_data_type.SparkSQLConnection =
  function(con, fields, ...)
    sapply(
      fields,
      function(x) {
        switch(
          class(x)[[1]],
          character = "STRING",
          Date =    "DATE",
          factor =  "STRING",
          integer = "INT",
          logical = "BOOLEAN",
          numeric = "DOUBLE",
          POSIXct = "TIMESTAMP",
          stop(
            "Unknown class ",
            paste(class(x), collapse = "/"), call. = FALSE))})

db_insert_into.SparkSQLConnection =
  function(con, table, values, ...) {
    mask = sapply(values, is.factor)
    values[mask] = lapply(values[mask], as.character)
    mask = sapply(values, is.character)
    values[mask] = lapply(values[mask], encodeString)
    tmp = tempfile()
    write.table(values, tmp, quote = FALSE, row.names = FALSE, col.names = FALSE, sep = "\001")
    stmt = build_sql("LOAD DATA LOCAL INPATH ", encodeString(tmp), " INTO TABLE ", ident(table), con = con)
    dbGetQuery(con, stmt)}

db_analyze.SparkSQLConnection =
  function(con, table, ...) TRUE

db_create_index.SparkSQLConnection =
  function(con, table, columns, name = NULL, ...)
    TRUE


tmp= new.env()
tmp$tables = list()

db_create_table.SparkSQLConnection =
  function(con, table, types, temporary = TRUE, ...) {
    table = tolower(table)
    if(temporary) tmp$tables = c(tmp$tables, table)
    temporary = FALSE
    NextMethod()}

db_save_query.SparkSQLConnection =
  function(con, sql, name, temporary = TRUE, ...){
    name = tolower(name)
    if(temporary) tmp$tables = c(tmp$tables, name)
    temporary = FALSE
    NextMethod()}

sql_escape_string.SparkSQLConnection =
  function(con, x)
    sql_quote(x, "'")

sql_escape_ident.SparkSQLConnection =
  function(con, x)
    sql_quote(x, " ")

tbl.src_SparkSQL =
  function(src, from, ...)
    tbl_sql("SparkSQL", src = src, from = tolower(from), ...)
