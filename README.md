


# dplyr.spark

This package implements a [`spark`](http://spark.apache.org/) backend for the [`dplyr`](http://github.com/hadley/dplyr) package, providing a powerful and intuitive DSL to manipulate large datasets on a powerful big data platform.  It is a simple package: simple to learn if you have any familiarity with `dplyr` or even just R and SQL, simple to deploy: just a few packages to install on a single machine, as long as your Spark installation comes with JDBC support -- or build it in, instructions below.
The current state of the project is:

 - most `dplyr` features supported
 - adds some `spark`-specific goodies, like *caching* tables.
 - can go succesfully through tutorials for `dplyr` like any other database backend^[with the exception of one bug to avoid which you need to run Spark from trunk or wait for version 1.5, see [SPARK-9221](https://issues.apache.org/jira/browse/SPARK-9921)]. 
 - not yet endowed with a thorugh test suite. Nonetheless we expect it to inherit much of its correctness, scalability and robustness from its main dependencies, `dplyr` and `spark`.
 - we don't recommend production use yet

## Installation

You need to [download spark](https://spark.apache.org/downloads.html) and [build it](https://spark.apache.org/docs/latest/building-spark.html) as follows

```
cd <spark root>
build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.4.0 -DskipTests -Phive -Phive-thriftserver clean package
```

It may work with other hadoop versions, but we need the hive and hive-thriftserver support. The package is able to start the thirft server but can also connect to a running one.

`dplyr.spark` has a few dependencies: get them with

```
install.packages(c("RJDBC", "dplyr", "DBI", "devtools"))
devtools::install_github("hadley/purrr")
```

Indirectly `RJDBC` needs `rJava`. Make sure that you have `rJava` working with:


```r
library(rJava)
.jinit()
```

This is only a test, in general you don't need it before loading `dplyr.spark`.

----------------

#### Mac Digression

On the mac `rJava` required two different versions of java installed, [for real](http://andrewgoldstone.com/blog/2015/02/03/rjava/), and in particular this shell variable set

```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home/jre/lib/server/
```
The specific path may be different, particularly the version numbers. To start Rstudio (optional, you can use a different GUI or none at all), which doesn't read environment variables, you can enter the following command:
```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_51.jdk/Contents/Home/jre/lib/server/ open -a rstudio
```
----------------

The `HADOOP_JAR` environment variable needs to be set to the main hadoop JAR file, something like `"<spark home>/assembly/target/scala-2.10/spark-assembly-1.4.1-SNAPSHOT-hadoop2.4.0.jar"` 

To start the thrift server from R, which happens by default when creating a `src_SparkSQL` object, you need one more variable set, `SPARK_HOME`, as the name suggests pointing to the root of the Spark installation. If you are connecting with a running server, you just need host and port information. Those can be stored in environment variable as well, see help documentation.




Then, to install from source:


```
devtools::install_github("RevolutionAnalytics/dplyr-spark@0.3.0", subdir = "pkg")
```

Linux package:


```
devtools::install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.3.0/dplyr.spark_0.3.0.tar.gz")
```

<!-- 
A windows package will be added in the near future.

Windows package:


```
install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.3.0/dplyr.spark_0.3.0.zip")
```

-->

The current version is 0.3.0 .

You can find a number of examples derived from @hadley's own tutorials for dplyr look under the [test](https://github.com/RevolutionAnalytics/dplyr-spark/tree/master/pkg/tests) directory, files `databases.R`, `window-functions.R` and `two-table.R`.

For new releases, subscribe to `dplyr-spark`'s Release notes [feed](https://github.com/RevolutionAnalytics/dplyr.spark/releases.atom) or join the [RHadoop Google group](https://groups.google.com/forum/#!forum/rhadoop). The latter is also the best place to get support, together with the [issue tracker](http://github.com/RevolutionAnalytics/dplyr.spark/issues))

