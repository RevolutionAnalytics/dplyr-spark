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

src_SparkSQL =
  function(
    host =
      detect(
        c(Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"), "localhost"),
        ~.!=""),
    port =
      detect(
        c(
          Sys.getenv("HIVE_SERVER2_THRIFT_PORT"), 10000),
        ~.!="")) {
    driverclass = "org.apache.hive.jdbc.HiveDriver"
    dr = JDBC(driverclass, Sys.getenv("HADOOP_JAR"))
    con =
      dbConnect(
        drv = dr,
        url = paste0("jdbc:hive2://", host, ":", port))
    con = new("SparkSQLConnection", con)
    src_sql(
      "SparkSQL",
      con,
      info = mget(names(formals()), sys.frame(sys.nframe())))}

src_desc.src_SparkSQL =
  function(x) {
    paste(x$info, collapse = ":")}

make.win.fun =
  function(f)
    function(x) {
      dplyr:::over(
        dplyr:::build_sql(
          dplyr:::sql(f),
          list(x)),
        dplyr:::partition_group(),
        NULL,
        frame = c(-Inf, Inf))}

src_translate_env.src_SparkSQL =
  function(x)
    sql_variant(
      scalar = base_scalar,
      aggregate =
        sql_translator(
          .parent = base_agg,
          n = function() sql("COUNT(*)"),
          sd =  sql_prefix("STDDEV_POP"),
          var = sql_prefix("VAR_SAMP")),
      window =
        sql_translator(
          .parent = base_win,
          n = function() sql("COUNT(*)"),
          sd =  make.win.fun("STDDEV_SAMP"),
          var = make.win.fun("VAR_SAMP")))

dedot = function(x) gsub("\\.", "_", x)

copy_to.src_SparkSQL =
  function(dest, df, name =  dedot(deparse(substitute(df))), ...) {
    force(name)
    names(df) = dedot(names(df))
    NextMethod(name = name)}

tbl.src_SparkSQL =
  function(src, from, ...){
    tbl_sql(
      "SparkSQL",
      src = src,
      from = if(is.sql(from)) from else tolower(from),
      ...)}

refresh = function(x, ...) UseMethod("refresh")

refresh.src_SparkSQL =
  function(x)
    do.call(src_SparkSQL, x$info)

