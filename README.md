


# VERY EXPERIMENTAL! FOLLOW THESE INSTRUCTIONS IF YOU ARE A DEVELOPER AND ARE INTERESTED IN HELPING OUT

# dplyr.spark


This package adds a spark src for the dplyr package

## Installation

You need to download spark and compile it as follows

```
cd <spark root>
build/mvn -Pyarn -Phadoop-2.6 -Dhadoop.version=2.6.0 -DskipTests -Phive -Phive-thriftserver clean package
```

Then start the thift service.

```
sbin/start-thriftserver.sh 
```

`dplyr.spark` had a few dependencies, `RJDBC`, `dplyr`, `DBI`. Indirectly `RJDBC` needs `rJava`. Make sure that you have `rJava` working with:


```r
library(rJava)
.jinit()
```

On the mac this required two different versions of java installed ([really](http://andrewgoldstone.com/blog/2015/02/03/rjava/)) and in particular this shell variable set

```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/
```

The specific path may be different, particularly the version numbers. To start Rstudio, which doesn't read environment variables, you can enter the following command:

```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/ open -a rstudio
```

Now you can download an install `dplyr.spark`. The `PATH` setting needs to include `<spark-home>/bin`. 


```r
library(dplyr)
library(dplyr.spark)

spark.src =  src_SparkSQL("localhost", "10000")
```

Now you can follow allong the `dplyr` tutorial, using this data source as opposed to, say, a `sqlite` source. 


