


# BETA QUALITY. KICK TIRES WITH CARE. NOT FOR PRODUCTION. FEEDBACK APPRECIATED

# dplyr.spark


This package adds a spark src for the dplyr package

## Installation

You need to [download spark](https://spark.apache.org/downloads.html) and [build it](https://spark.apache.org/docs/latest/building-spark.html) as follows


```
cd <spark root>
build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests -Phive -Phive-thriftserver clean package
```

It may work with other versions, but we need the hive and hive-thriftserver support. Then start the thift service.

```
sbin/start-thriftserver.sh  

```

I've had to tamper with some memory options. Unless you see an out-of-memory error, you probably don't need them, but if you do here's how I start it:


```
sbin/start-thriftserver.sh  --driver-memory 1G --executor-memory 2G

```

`dplyr.spark` has a few dependencies: get them with

```
install.packages(c("RJDBC", "dplyr", "DBI", "devtools"))
devtools::install_github("hadley/purrr") #soon to be on CRAN, then we don't need devtools
```

Indirectly `RJDBC` needs `rJava`. Make sure that you have `rJava` working with:


```r
library(rJava)
.jinit()
```

This is only a test, in general you don't need it before loading `dplyr.spark`.

----------------

#### Mac Digression

On the mac this required two different versions of java installed, [for real](http://andrewgoldstone.com/blog/2015/02/03/rjava/), and in particular this shell variable set
```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/
```
The specific path may be different, particularly the version numbers. To start Rstudio (optional, you can use a different GUI or none at all), which doesn't read environment variables, you can enter the following command:
```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/ open -a rstudio
```
----------------

The `HADOOP_JAR` environment variable needs to be set to the main hadoop JAR file, something like `"<spark home>/assembly/target/scala-2.10/spark-assembly-1.4.0-SNAPSHOT-hadoop2.6.0.jar"` 

Two more variables whose contents are used as defaults when creating a `src` for this backend are
`HIVE_SERVER2_THRIFT_BIND_HOST`, the address of the host running the thrift server, and `HIVE_SERVER2_THRIFT_PORT`, the port on which the thift server is accepting connections. If you don't specify these variables, the default values are `"localhost"` and `10000` respectively. They can also be provided as arguments to `src_SparkSQL`.

To install, first install and load `devtools`


```r
install.packages("devtools")
library(devtools)
```



Then, to install from source:


```
install_github("RevolutionAnalytics/dplyr-spark@0.2.2", subdir = "pkg")
```

Linux package:


```
install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.2.2/dplyr.spark_0.2.2.tar.gz")
```

<!-- 
A windows package will be added in the near future.

Windows package:


```
install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.2.2/dplyr.spark_0.2.2.zip")
```

-->

The current version is 0.2.2 .



```r
library(dplyr)
library(dplyr.spark)

spark.src = src_SparkSQL("localhost", "10000")
```



For new releases, subscribe to `dplyr-spark`'s Release notes [feed](https://github.com/RevolutionAnalytics/dplyr.spark/releases.atom) or join the [RHadoop Google group](https://groups.google.com/forum/#!forum/rhadoop). The latter is also the best place to get support, together with the [issue tracker](http://github.com/RevolutionAnalytics/dplyr.spark/issues))

