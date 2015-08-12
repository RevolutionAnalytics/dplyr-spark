
over  =
  function (expr, partition = NULL, order = NULL, frame = NULL)
  {
    args <- (!is.null(partition)) + (!is.null(order)) + (!is.null(frame))
    if (args == 0) {
      stop("Must supply at least one of partition, order, frame",
           call. = FALSE)
    }
    if (!is.null(partition)) {
      partition <- build_sql("PARTITION BY ",sql_vector(partition,
                                                         collapse = ", ",  parens = FALSE))
    }
    if (!is.null(order)) {
      order <- build_sql("ORDER BY ", sql_vector(order, collapse = ", ", parens = FALSE))
    }
    if (!is.null(frame)) {
      if (is.numeric(frame))
        frame <- rows(frame[1], frame[2])
      frame <- build_sql("ROWS ", frame)
    }
    over <- sql_vector(compact(list(partition, order, frame)),
                       parens = TRUE)
    build_sql(expr, " OVER ", over)
  }

environment (over) = environment(select_)

assignInNamespace(
  x = "over",
  ns = "dplyr",
  value = over)


library(Lahman)
batting = copy_to(my_db, Batting)
batting <- select(batting, playerid, yearid, teamid, g, ab:h)
batting <- arrange(batting, playerid, yearid, teamid)
players <- group_by(batting, playerid)

# For each player, find the two years with most hits
filter(players, min_rank(desc(h)) <= 2 & h > 0)
# Within each player, rank each year by the number of games played
mutate(players, g_rank = min_rank(g))

# For each player, find every year that was better than the previous year
#broken
filter(players, g > lag(g))
# For each player, compute avg change in games played per year
#broken
mutate(players, g_change = (g - lag(g)) / (yearid - lag(yearid)))

# For each player, find all where they played more games than average
filter(players, g > mean(g))
# For each, player compute a z score based on number of games played
mutate(players, g_z = (g - mean(g)) / sd(g))
