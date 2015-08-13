


library(Lahman)
batting = copy_to(my_db, Batting)
batting = tbl(my_db, "batting")
batting <- select(batting, playerid, yearid, teamid, g, ab:h)
batting <- arrange(batting, playerid, yearid, teamid)
players <- group_by(batting, playerid)

# For each player, find the two years with most hits
filter(players, min_rank(desc(h)) <= 2 & h > 0)
# Within each player, rank each year by the number of games played
mutate(players, g_rank = min_rank(g))

# For each player, find every year that was better than the previous year
filter(players, g > lag(g))
# For each player, compute avg change in games played per year
mutate(players, g_change = (g - lag(g)) / (yearid - lag(yearid)))

# For each player, find all where they played more games than average
filter(players, g > mean(g))
# For each, player compute a z score based on number of games played
mutate(players, g_z = (g - mean(g)) / sd(g))
