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


setClass(
  "SparkSQLConnection",
  slots = list(tmptables = "environment"),
  contains = "JDBCConnection")

src_SparkSQL =
  function(host = "localhost", port = 10000) {
    driverclass = "org.apache.hive.jdbc.HiveDriver"
    dr = JDBC(driverclass, Sys.getenv("HADOOP_JAR"))
    con =
      dbConnect(
        drv = dr,
        url = paste0("jdbc:hive2://", host, ":", port))
    tmptables = new.env()
    reg.finalizer(
      tmptables,
      function(en)
        sapply(ls(en), function(x) db_drop_table(con, x)))
    con = new("SparkSQLConnection", con, tmptables = tmptables )
    src_sql(
      "SparkSQL",
      con,
      info = mget(names(formals()), sys.frame(sys.nframe())))}

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

copy_to.src_SparkSQL =
  function(dest, df, name = gsub("\\.", "_", deparse(substitute(df))), ...) {
    NextMethod()}

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

db_explain.SparkSQLConnection = dplyr:::db_explain.MySQLConnection

db_begin.SparkSQLConnection =
  function(con, ...) TRUE

db_commit.SparkSQLConnection =
  function(con, ...) TRUE

db_rollback.SparkSQLConnection =
  function(con, ...) TRUE

setMethod(
  "dbDataType",
  signature = "SparkSQLConnection",
  function(dbObj, obj, ...)
    switch(
      class(obj)[[1]],
      character = "STRING",
      Date =    "DATE",
      factor =  "STRING",
      integer = "INT",
      logical = "BOOLEAN",
      numeric = "DOUBLE",
      POSIXct = "TIMESTAMP",
      raw = "BINARY",
      stop(
        "Can't map",
        paste(class(obj), collapse = "/"),
        "to a supported type")))

convert.from.DB =
  function(type) {
    switch(
      tolower(type),
      tinyint = as.integer,
      smallint = as.integer,
      int = as.integer,
      bigint = as.numeric,
      boolean = as.logical,
      float = as.double,
      double = as.double,
      string = as.character,
      binary = as.raw,
      timestamp = as.POSIXct,
      decimal = as.double,
      date = as.Date,
      varchar = as.character,
      char = as.character,
      stop("Don't know what to map ", type, " to"))}

#modeled after db_insert_into methods in http://github.com/hadley/dplyr,
#under MIT license
db_insert_into.SparkSQLConnection =
  function(con, table, values, ...) {
    mask = sapply(values, is.factor)
    values[mask] = lapply(values[mask], as.character)
    mask = sapply(values, is.character)
    values[mask] = lapply(values[mask], encodeString)
    tmp = tempfile()
    write.table(
      values,
      tmp,
      quote = FALSE,
      row.names = FALSE,
      col.names = FALSE,
      sep = "\001")
    dbGetQuery(
      con,
      build_sql(
        "LOAD DATA LOCAL INPATH ",
        encodeString(tmp),
        " INTO TABLE ",
        ident(table),
        con = con))}

db_analyze.SparkSQLConnection =
  function(con, table, ...) TRUE

db_create_index.SparkSQLConnection =
  function(con, table, columns, name = NULL, ...)
    TRUE

db_create_table.SparkSQLConnection =
  function(con, table, types, temporary = TRUE, ...) {
    table = tolower(table)
    if(temporary) con@tmptables[[table]] = TRUE
    temporary = FALSE
    NextMethod()}

db_save_query.SparkSQLConnection =
  function(con, sql, name, temporary = TRUE, ...){
    name = tolower(name)
    if(temporary) con@tmptables[[name]] = TRUE
    temporary = FALSE
    NextMethod()}

db_explain.SparkSQLConnection =
  function(con, sql, ...)
    dbGetQuery(
      con,
      build_sql("EXPLAIN ", sql))

sql_escape_string.SparkSQLConnection =
  function(con, x)
    sql_quote(x, "'")

sql_escape_ident.SparkSQLConnection =
  function(con, x)
    sql_quote(gsub("\\.", "_", tolower(x)), "`")

fully_qualify =
  function(fields, tables) {
    paste(tables, fields, sep =".") }

#modeled after sql_join methods in http://github.com/hadley/dplyr,
#under MIT license
sql_join.SparkSQLConnection =
  function (con, x, y, type = "inner", by = NULL, ...)  {
    join =
      switch(
        type,
        left = sql("LEFT"),
        inner = sql("INNER"),
        right = sql("RIGHT"),
        full = sql("FULL"),
        stop("Unknown join type:", type, call. = FALSE))
    by = dplyr:::common_by(by, x, y)
    x_names = dplyr:::auto_names(x$select)
    y_names = dplyr:::auto_names(y$select)
    uniques =
      dplyr:::unique_names(
        x_names,
        y_names,
        by$x[by$x == by$y],
        "_x",
        "_y")
    name.left = dplyr:::random_table_name()
    name.right = dplyr:::random_table_name()
    if (is.null(uniques)){
      sel_vars =
        c(
          fully_qualify(x_names, name.left),
          setdiff(y_names, x_names))}
    else {
      x = update(x, select = setNames(x$select, uniques$x))
      y = update(y, select = setNames(y$select, uniques$y))
      by$x = unname(uniques$x[by$x])
      by$y = unname(uniques$y[by$y])
      sel_vars =
        c(
          fully_qualify(uniques$x, name.left),
          setdiff(uniques$y, x_names))}
    on =
      dplyr:::sql_vector(
        paste0(
          sql_escape_ident(con, fully_qualify(by$x, name.left)),
          " = ",
          sql_escape_ident(con, fully_qualify(by$y, name.right)),
          collapse = " AND "),
        parens = TRUE)
    cond = build_sql("ON ", on, con = con)
    from =
      build_sql(
        sql_subquery(con, x$query$sql, name.left),
        "\n", join, " JOIN \n",
        sql_subquery(con, y$query$sql, name.right),
        "\n", cond, con = con)
    attr(from, "vars") = lapply(sel_vars, as.name)
    class(from) = c("join", class(from))
    from}

#modeled after sql_semi_join methods in http://github.com/hadley/dplyr,
#under MIT license
sql_semi_join.SparkSQLConnection =
  function (con, x, y, anti = FALSE, by = NULL, ...) {
    if(anti) stop("antijoins not implemented yet")
    by = dplyr:::common_by(by, x, y)
    left = escape(ident("L_LEFT"), con = con)
    right = escape(ident("R_RIGHT"), con = con)
    on =
      dplyr:::sql_vector(
        paste0(
          left, ".",
          sql_escape_ident(con, by$x), " = ",
          right, ".",
          sql_escape_ident(con, by$y)),
        collapse = " AND ",
        parens = TRUE)
    from =
      build_sql(
        "SELECT * FROM ",
        sql_subquery(con, x$query$sql, "L_LEFT"), "\n",
        "WHERE ",
        if (anti) sql("NOT "),
        "EXISTS (\n", "  SELECT 1 FROM ",
        sql_subquery(con, y$query$sql, "R_RIGHT"), "\n",
        "  WHERE ", on, ")")
    attr(from, "vars") = x$select
    from}

tbl.src_SparkSQL =
  function(src, from, ...)
    tbl_sql("SparkSQL", src = src, from = tolower(from), ...)

collect.tbl_SparkSQL =
  function(x, ...) {
    x = compute(x)
    res = NextMethod(x = x)
    db.types = DBI::dbGetQuery(x$src$con, paste("describe", x$from)
                               )$data_type
    db_drop_table(table = paste0('`', x$from,'`'), con = x$src$con)
    sapply(
      seq_along(res),
      function(i)
        res[[i]] <<- convert.from.DB(db.types[i])(res[[i]]))
    res}

#modeled after mutate_ methods in http://github.com/hadley/dplyr,
#under MIT license
mutate_.tbl_SparkSQL =
  function (.data, ..., .dots) {
    dots <- lazyeval::all_dots(.dots, ..., all_named = TRUE)
    input = lapply(dots, function(x) partial_eval(x$expr, .data, map(dots, function(x) call("(", x$expr))))
    #input = lapply(dots, function(x) partial_eval(x$expr, .data, map(dots, "expr")))
    .data$mutate <- TRUE
    new <- update(.data, select = c(.data$select, input))
    if (dplyr:::uses_window_fun(input, .data)) {
      collapse(new) }
    else {
      new}}

#modeled after union methods in http://github.com/hadley/dplyr,
#under MIT license
union.tbl_SparkSQL =
  function (x, y, copy = FALSE, ...) {
    y = dplyr:::auto_copy(x, y, copy)
    sql = sql_set_op(x$src$con, x, y, "UNION ALL")
    dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = groups(x)) }

