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

# derivative of dplyr introductory material, http://github.com/hadley/dplyr
# presumably under MIT licenselibrary(dplyr)

library(dplyr)
library(dplyr.spark)
Sys.setenv(
  HADOOP_JAR = "../spark/assembly/target/scala-2.10/spark-assembly-1.5.0-SNAPSHOT-hadoop2.4.0.jar",
  SPARK_HOME = "../spark")

my_db = src_SparkSQL()


library(Lahman)
batting = copy_to(my_db, Batting)
batting = tbl(my_db, "batting")
batting <- select(batting, playerid, yearid, teamid, g, ab:h)
batting <- arrange(batting, playerid, yearid, teamid)
players <- group_by(batting, playerid)
cache(batting)
cache(players)
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
