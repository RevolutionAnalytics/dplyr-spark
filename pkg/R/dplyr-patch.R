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

over  =
  function (expr, partition = NULL, order = NULL, frame = NULL)  {
    args = (!is.null(partition)) + (!is.null(order)) + (!is.null(frame))
    if (args == 0) {
      stop("Must supply at least one of partition, order, frame",
           call. = FALSE) }
    if (!is.null(partition)) {
      partition =
        build_sql(
          "PARTITION BY ",
          sql_vector(partition, collapse = ", ",  parens = FALSE))}
    if (!is.null(order)) {
      order = build_sql("ORDER BY ", sql_vector(order, collapse = ", ", parens = FALSE))}
    if (!is.null(frame)) {
      if (is.numeric(frame))
        frame = rows(frame[1], frame[2])
      frame = build_sql("ROWS ", frame) }
    over =
      sql_vector(
        compact(list(partition, order, frame)),
        parens = TRUE)
    build_sql(expr, " OVER ", over)}

environment (over) = environment(select_)

.onLoad = function(._,.__) {
  assign(
    'n_distinct',
    function(x) {
      build_sql("COUNT(DISTINCT ", x, ")")},
    envir=base_agg)
  assignInNamespace(
    x = "over",
    ns = "dplyr",
    value = over)
  # doesn't seem necessary anymore
  #  assignInNamespace(
  #     "unique_name",
  #     function()
  #       paste0("tmp", strsplit(as.character(runif(1)), "\\.")[[1]][2]),
  #     ns = "dplyr")
}