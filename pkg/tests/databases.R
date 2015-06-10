# derivative of dplyr introductory material, http://github.com/hadley/dplyr
# presumably under MIT license

library(dplyr)
library(dplyr.spark)
Sys.setenv(
  HADOOP_JAR =
    "/Users/antonio/Projects/Revolution/spark/assembly/target/scala-2.10/spark-assembly-1.4.0-SNAPSHOT-hadoop2.6.0.jar")
assignInNamespace(
  "unique_name",
  function()
    paste0("tmp", strsplit(as.character(runif(1)), "\\.")[[1]][2]),
  ns = "dplyr")
assign(
  'n_distinct',
  function(x) {
    build_sql("COUNT(DISTINCT ", x, ")")},
  envir=base_agg)

my_db = src_SparkSQL()

library(nycflights13)
flights_SparkSQL = copy_to(my_db, flights, temporary = TRUE)
flights_SparkSQL = tbl(my_db, "flights")
flights_SparkSQL

tbl(my_db, sql("SELECT * FROM flights"))

## ------------------------------------------------------------------------
select(flights_SparkSQL, year:day, dep_delay, arr_delay)
filter(flights_SparkSQL, dep_delay > 240)
arrange(flights_SparkSQL, year, month, day)
mutate(flights_SparkSQL, speed = air_time / distance)
summarise(flights_SparkSQL, delay = mean(dep_time))

c1 = filter(flights_SparkSQL, year == 2013, month == 1, day == 1)
c2 = select(c1, year, month, day, carrier, dep_delay, air_time, distance)
c3 = mutate(c2, speed = distance / air_time * 60)
c4 = arrange(c3, year, month, day, carrier)

c4

collect(c4)

c4$query

explain(c4)

by_tailnum = group_by(flights_SparkSQL, tailnum)
delay = summarise(by_tailnum,
  count = n(),
  dist = mean(distance),
  delay = mean(arr_delay))
delay = filter(delay, count > 20, dist < 2000)
delay_local = collect(delay)
delay_local


daily = group_by(flights_SparkSQL, year, month, day)

#broken
bestworst = daily %>%
  select(flight, arr_delay) %>%
  filter(arr_delay == min(arr_delay) || arr_delay == max(arr_delay))
bestworst

#broken
ranked = daily %>%
  select(arr_delay) %>%
  mutate(rank = rank(desc(arr_delay)))
ranked

summarise(daily, arr_delay = mean(arr_delay))


library(ggplot2)

#from the tutorial for data frames


dim(flights_SparkSQL)
head(flights_SparkSQL)

filter(flights_SparkSQL, month == 1, day == 1)

arrange(flights_SparkSQL, year, month, day)

arrange(flights_SparkSQL, desc(arr_delay))

select(flights_SparkSQL, year, month, day)
select(flights_SparkSQL, year:day)
select(flights_SparkSQL, -(year:day))

select(flights_SparkSQL, tail_num = tailnum)

rename(flights_SparkSQL, tail_num = tailnum)

distinct(select(flights_SparkSQL, tailnum))
distinct(select(flights_SparkSQL, origin, dest))

mutate(
  flights_SparkSQL,
  gain = arr_delay - dep_delay,
  speed = distance / air_time * 60)

#broken:sequential eval
mutate(
  flights_SparkSQL,
  gain = arr_delay - dep_delay,
  gain_per_hour = gain / (air_time / 60))

#alternative
flights_SparkSQL %>%
  mutate(gain = arr_delay - dep_delay) %>%
  compute %>%
  mutate(gain_per_hour = gain / (air_time / 60))

#broken:sequential eval
transmute(
  flights_SparkSQL,
  gain = arr_delay - dep_delay,
  gain_per_hour = gain / (air_time / 60))

summarise(
  flights_SparkSQL,
  delay = mean(dep_delay))

#not in dplyr for sql
slice(flights_SparkSQL, 1:10)
sample_n(flights_SparkSQL, 10)
sample_frac(flights_SparkSQL, 0.01)

by_tailnum = group_by(flights_SparkSQL, tailnum)
delay = summarise(by_tailnum,
                   count = n(),
                   dist = mean(distance),
                   delay = mean(arr_delay))
delay = filter(delay, count > 20, dist < 2000)

ggplot(
  collect(delay),
  aes(dist, delay)) +
  geom_point(aes(size = count), alpha = 1/2) +
  geom_smooth() +
  scale_size_area()

destinations = group_by(flights_SparkSQL, dest)
summarise(
  destinations,
  planes = n_distinct(tailnum),
  flights = n())

daily = group_by(flights_SparkSQL, year, month, day)
(per_day   = summarise(daily, flights = n()))
(per_month = summarise(per_day, flights = sum(flights)))
(per_year  = summarise(per_month, flights = sum(flights)))

a1 = group_by(flights_SparkSQL, year, month, day)
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
      group_by(flights_SparkSQL, year, month, day),
      arr_delay, dep_delay),
    arr = mean(arr_delay),
    dep = mean(dep_delay)),
  arr > 30 | dep > 30)

flights_SparkSQL %>%
  group_by(year, month, day) %>%
  select(arr_delay, dep_delay) %>%
  summarise(
    arr = mean(arr_delay),
    dep = mean(dep_delay)) %>%
  filter(arr > 30 | dep > 30)




