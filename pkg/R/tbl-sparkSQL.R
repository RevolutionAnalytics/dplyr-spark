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


collect.tbl_SparkSQL =
  function(x, ...) {
    xs = compute(suppressMessages(top_n(x, 1)), temporary = FALSE)
    res = dplyr:::collect.tbl_sql(x, ...)
    db.types =
      DBI::dbGetQuery(xs$src$con, paste("describe", xs$from))$data_type
    db_drop_table(table = paste0('`', xs$from,'`'), con = xs$src$con)
    sapply(
      seq_along(res),
      function(i)
        res[[i]] <<- convert.from.DB(db.types[i])(res[[i]]))
    res}

#modeled after mutate_ methods in http://github.com/hadley/dplyr,
#under MIT license
mutate_.tbl_SparkSQL =
  function (.data, ..., .dots) {
    dots = lazyeval::all_dots(.dots, ..., all_named = TRUE)
    input = partial_eval(dots, .data)
    for(i in 1:length(input))
      input = lapply(input, function(x) partial_eval(x, .data, input))
    .data$mutate = TRUE
    no.dup.select = .data$select[!as.character(.data$select) %in% names(input)]
    new = update(.data, select = c(no.dup.select, input))
    if (dplyr:::uses_window_fun(input, .data)) {
      collapse(new) }
    else {
      new}}


filter_.tbl_SparkSQL =
  function (.data, ..., .dots)   {
    dots = lazyeval::all_dots(.dots, ...)
    input = partial_eval(dots, .data)
    if(
      any(
        names(.data$select) %in%
        flatten(all.vars(input[[1]]))))
      .data = collapse(.data)
    update(.data, where = c(.data$where, input))}

assert.compatible =
  function(x, y)
    if(suppressWarnings(!all(colnames(x) == colnames(y))))
      stop("Tables not compatible")

#modeled after union methods in http://github.com/hadley/dplyr,
#under MIT license
union.tbl_SparkSQL =
  function (x, y, copy = FALSE, ...) {
    assert.compatible(x, y)
    y = dplyr:::auto_copy(x, y, copy)
    sql = sql_set_op(x$src$con, x, y, "UNION ALL")
    distinct(dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = groups(x)))}

#modeled after intersect methods in http://github.com/hadley/dplyr,
#under MIT license
intersect.tbl_SparkSQL =
  function (x, y, copy = FALSE, ...){
    assert.compatible(x, y)
    xy = inner_join(x, y, copy = copy)
    select_(xy, .dots = setNames(colnames(xy)[1:(NCOL(xy)/2)], colnames(x)))}

#modeled after join methods in http://github.com/hadley/dplyr,
#under MIT license
some_join =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ..., type) {
    by = dplyr:::common_by(by, x, y)
    y = dplyr:::auto_copy(x, y, copy, indexes = if (auto_index)
      list(by$y))
    sql = dplyr:::sql_join(x$src$con, x, y, type = type, by = by)
    dplyr:::update.tbl_sql(tbl(x$src, sql), group_by = groups(x))}

right_join.tbl_SparkSQL =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ...) {
    some_join(x = x, y = y, by = by, copy = copy, auto_index = auto_index, ..., type = "right")}

full_join.tbl_SparkSQL =
  function (x, y, by = NULL, copy = FALSE, auto_index = FALSE, ...) {
    some_join(x = x, y = y, by = by, copy = copy, auto_index = auto_index, ..., type = "full")}

# refresh.tbl_sql =
#   function(x, src = refresh(x$src), ...) {
#     tbl(src, x$query$sql)}

cache =
  function(tbl) {
    DBI::dbSendQuery(tbl$src$con, paste("CACHE TABLE", tbl$from))
    tbl}