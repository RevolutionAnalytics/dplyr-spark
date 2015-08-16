# derivative of dplyr introductory material, http://github.com/hadley/dplyr
# presumably under MIT license

library(dplyr)
library(dplyr.spark)
Sys.setenv(
  HADOOP_JAR = "../spark/assembly/target/scala-2.10/spark-assembly-1.5.0-SNAPSHOT-hadoop2.4.0.jar",
  SPARK_HOME = "../spark")

my_db = src_SparkSQL()

library(nycflights13)
flights = copy_to(my_db, flights, temporary = TRUE)
flights = tbl(my_db, "flights")
flights
cache(flights)

tbl(my_db, sql("SELECT * FROM flights"))

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

collect(c4)

c4$query

explain(c4)

by_tailnum = group_by(flights, tailnum)
delay = summarise(by_tailnum,
  count = n(),
  dist = mean(distance),
  delay = mean(arr_delay))
delay = filter(delay, count > 20, dist < 2000)
delay_local = collect(delay)
delay_local


daily = group_by(flights, year, month, day)

bestworst = daily %>%
  select(flight, arr_delay) %>%
  filter(arr_delay == min(arr_delay) || arr_delay == max(arr_delay))
bestworst

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
  speed = distance / air_time * 60)

mutate(
  flights,
  gain = arr_delay - dep_delay,
  gain_per_hour = gain / (air_time / 60))

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

destinations = group_by(flights, dest)
summarise(
  destinations,
  planes = n_distinct(tailnum),
  flights = n())

daily = group_by(flights, year, month, day)
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
      group_by(flights, year, month, day),
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




