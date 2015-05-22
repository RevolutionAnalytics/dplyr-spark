


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

`dplyr.spark` has a few dependencies: get them with

```
install.packages(c(`RJDBC`, `dplyr`, `DBI`))
```

Indirectly `RJDBC` needs `rJava`. Make sure that you have `rJava` working with:


```r
library(rJava)
.jinit()
```

This is only a test, in general you don't need it before loading `dplyr.spark`.

<ul> <li>Mac digression :

On the mac this required two different versions of java installed, [for real](http://andrewgoldstone.com/blog/2015/02/03/rjava/), and in particular this shell variable set

```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/
```

The specific path may be different, particularly the version numbers. To start Rstudio (optional, you can use a different GUI or none at all), which doesn't read environment variables, you can enter the following command:

```
DYLD_FALLBACK_LIBRARY_PATH=/Library/Java/JavaVirtualMachines/jdk1.8.0_45.jdk/Contents/Home/jre/lib/server/ open -a rstudio
```
</li></ul>

The `PATH` environment variable needs to include `<spark-home>/bin`. 

To install, first install and load `devtools`


```r
install.packages("devtools")
library(devtools)
```



Then, to install from source:


```
install_github("RevolutionAnalytics/dplyr-spark@0.1.0", subdir = "pkg")
```

Binary packages will be added in the near future.

<!-- Linux package:


```
install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.1.0/dplyr.spark_0.1.0.tar.gz")
```

Windows package:


```
install_url(
  "https://github.com/RevolutionAnalytics/dplyr-spark/releases/download/0.1.0/dplyr.spark_0.1.0.zip")
```

-->

The current version is 0.1.0 .

While this package was first developed to support the activities of the RHadoop project, it's not part of it nor related to Hadoop or big data. While it has been in use for a few years to test packages used in production, version 3.0.0 marks the first version of the project that's offered for general use and as such it went through a major API re-design. Hence, versions 3.x.y should be considered beta  releases and no backward compatibility guarantees are offered, as it is customary in [semantic versioning](http://semver.org) for 0.x.y releases. We will switch to the normal major/minor/hotfix releases from version 4.



```r
library(dplyr)
library(dplyr.spark)

spark.src = src_SparkSQL("localhost", "10000")
```


Monkey patches for `dplyr`: 
 - `assignInNamespace("unique_name", function() paste0("tmp", strsplit(as.character(runif(1)), "\\.")[[1]][2]), ns = "dplyr")`. Makes subqueries work. See issue #4. Can't make it work in the package, you need to enter it at top level.
 - `assign('n_distinct', function(x) {build_sql("COUNT(DISTINCT ", x, ")")}, envir=base_agg)` makes `n_distinct` work.
 
Known limitations: see issue #3 and other issues, please do before you get frustrated. In particular the current implementation does not clean up temp tables. Working on it. Prepare to drop many tables.

Now you can follow along the `dplyr` tutorial, using this data source as opposed to, say, a `sqlite` source.



For new releases, subscribe to `dplyr-spark`'s Release notes [feed](https://github.com/RevolutionAnalytics/dplyr.spark/releases.atom) or join the [RHadoop Google group](https://groups.google.com/forum/#!forum/rhadoop). The latter is also the best place to get support (well, second only to the [issue tracker](http://github.com/RevolutionAnalytics/dplyr.spark/issues))



