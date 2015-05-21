


# BETA QUALITY. KICK TIRES WITH CARE. NOT FOR PRODUCTION. FEEDBACK APPRECIATED

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

Now you can download an install `dplyr.spark`. The `PATH` setting needs to include `<spark-home>/bin`. 


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


