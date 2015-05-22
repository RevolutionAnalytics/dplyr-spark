\name{src_SparkSQL}
\alias{src_SparkSQL}

\title{
Create a dplyr src backed by SparkSQL
}
\description{
A dplyr src backed by SparkSQL, supports most operations other databases do with the familiar dplyr syntax and semantics.
}
\usage{
src_SparkSQL(host = localhost, port = 10000)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{host}{
 The address of the host running the thift server (see Details)
}
  \item{port}{
 The port on which the thirft server is listening
}

}
\details{
You need to have the hive-thiftserver running, see \url{https://spark.apache.org/docs/latest/sql-programming-guide.html}
}
\value{
An object of class \code{src_SparkSQL}. Use it like any other src in dplyr/
}
\author{
Revolution Analytics}


\examples{
   #See the \code{dplyr} tutorial. Limitations are documented at
   #http://github.com/RevolutionAnalytics/dplyr-spark/issues/3
}
