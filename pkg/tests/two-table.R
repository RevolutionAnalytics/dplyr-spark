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
library(purrr)

my_db = src_SparkSQL()

library(nycflights13)

#first time around
flights = copy_to(my_db, flights, temporary = TRUE)
airlines = copy_to(my_db, airlines, temporary = TRUE)
weather = copy_to(my_db, weather, temporary = TRUE)
planes = copy_to(my_db, planes, temporary = TRUE)
airports = copy_to(my_db, airports, temporary = TRUE)

#thereon
flights = tbl(my_db, "flights")
airlines = tbl(my_db, "airlines")
weather = tbl(my_db, "weather")
planes = tbl(my_db, "planes")
airports = tbl(my_db, "airports")

flights2 =
  flights %>%
  select(year:day, hour, origin, dest, tailnum, carrier)

flights2 %>%
  left_join(airlines)

flights2 %>% left_join(weather)

flights2 %>% left_join(planes, by = "tailnum")

flights2 %>% left_join(airports, c("dest" = "faa"))
# not in dplyr dplyr/#1181
# flights2 %>% left_join(airports, dest == faa)

flights2 %>% left_join(airports, c("origin" = "faa"))


(df1 = data_frame(x = c(1, 2), y = 2:1))
(df2 = data_frame(x = c(1, 3), a = 10, b = "a"))

df1 = copy_to(my_db, df1, temporary = TRUE)
df2 = copy_to(my_db, df2, temporary = TRUE)

df1 = tbl(my_db, "df1")
df2 = tbl(my_db, "df2")

collect(df1 %>% inner_join(df2))

collect(df1 %>% left_join(df2))

collect(df1 %>% right_join(df2))

collect(df2 %>% left_join(df1))

collect(df1 %>% full_join(df2))

planes = copy_to(my_db, planes)
planes = tbl(my_db, "planes")

flights %>%
  anti_join(planes, by = "tailnum") %>%
  count(tailnum, sort = TRUE)

df1 %>% nrow()
#returns NA
df1 %>% inner_join(df2, by = "x") %>% nrow()
#returns NA
df1 %>% semi_join(df2, by = "x") %>% nrow()

#need better examples here
intersect(df1, df2)
union(df1, df2)
#setdiff(df1, df2)
#setdiff(df2, df1)

full_join(df1, df2)


