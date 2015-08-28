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
#
# derivative of dplyr introductory material, http://github.com/hadley/dplyr
# presumably under MIT license

library(dplyr)
library(dplyr.spark)

my_db = src_SparkSQL()

library(nycflights13)
flights = {
  if(db_has_table(my_db$con, "flights"))
    tbl(my_db, "flights")
  else
    copy_to(my_db, flights, temporary = FALSE)}
flights
cache(flights)

## ------------------------------------------------------------------------
select(flights, year:day, dep_delay, arr_delay)
filter(flights, dep_delay > 240)
arrange(flights, year, month, day)
mutate(flights, speed = air_time / distance)
summarise(flights, delay = mean(dep_time))

c1 = filter(flights, year == 2013, month == 1, day == 1)
c2 = select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 = mutate(c2, speed = distance / air_time * 60)
c4 = arrange(c3, year, month, day, carrier)

c4

collect(c4, name = "c4", temporary = FALSE)
c4$query$sql

explain(c4)

db_drop_table(my_db$con, "c4")

flights %>%
filter(year == 2013, month == 1, day == 1) %>%
select(year, month, day, carrier, dep_delay, air_time, distance)  %>%
mutate(speed = distance / air_time * 60) %>%
arrange(year, month, day, carrier)


daily = group_by(flights, year, month, day)

bestworst =
  daily %>%
  select(flight, arr_delay) %>%
  filter(arr_delay == min(arr_delay) || arr_delay == max(arr_delay))
bestworst

bestworst$query$sql

ranked = daily %>%
  select(arr_delay) %>%
  mutate(rank = rank(desc(arr_delay)))
ranked

summarise(daily, arr_delay = mean(arr_delay))


library(ggplot2)

#from the tutorial for data frames


dim(flights)
head(flights)

filter(flights, month == 1, day == 1)

arrange(flights, year, month, day)

arrange(flights, desc(arr_delay))

select(flights, year, month, day)
select(flights, year:day)
select(flights, -(year:day))

select(flights, tail_num = tailnum)

rename(flights, tail_num = tailnum)

distinct(select(flights, tailnum))
distinct(select(flights, origin, dest))

mutate(
  flights,
  gain = arr_delay - dep_delay,
  speed = distance / air_time * 60) %>%
  select(flight, gain, speed)

mutate(
  flights,
  gain = arr_delay - dep_delay,
  gain_per_hour = gain / (air_time / 60))%>%
  select(flight, gain, gain_per_hour)

transmute(
  flights,
  gain = arr_delay - dep_delay,
  gain_per_hour = gain / (air_time / 60))

summarise(
  flights,
  delay = mean(dep_delay))

#not in dplyr for sql
# slice(flights, 1:10)
# sample_n(flights, 10)
# sample_frac(flights, 0.01)



by_tailnum = group_by(flights, tailnum)
delay =
  summarise(
    by_tailnum,
    count = n(),
    dist = mean(distance),
    delay = mean(arr_delay))
delay = filter(delay, count > 20, dist < 2000)
delay_local = collect(delay)
delay_local

ggplot(
  collect(delay),
  aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area()

destinations = group_by(flights, dest)
summarise(
  destinations,
  planes = n_distinct(tailnum),
  flights = n())

(per_day   = summarise(daily, flights = n()))
(per_month = summarise(per_day, flights = sum(flights)))
(per_year  = summarise(per_month, flights = sum(flights)))

a1 = group_by(flights, year, month, day)
a2 = select(a1, arr_delay, dep_delay)
a3 =
  summarise(
    a2,
    arr = mean(arr_delay),
    dep = mean(dep_delay))
a4 = filter(a3, arr > 30 | dep > 30)
a4

filter(
  summarise(
    select(
      daily,
      arr_delay, dep_delay),
    arr = mean(arr_delay),
    dep = mean(dep_delay)),
  arr > 30 | dep > 30)

flights %>%
  group_by(year, month, day) %>%
  select(arr_delay, dep_delay) %>%
  summarise(
    arr = mean(arr_delay),
    dep = mean(dep_delay)) %>%
  filter(arr > 30 | dep > 30)




