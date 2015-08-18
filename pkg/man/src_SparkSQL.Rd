\name{src_SparkSQL}
\alias{src_SparkSQL}

\title{
Create a dplyr src backed by SparkSQL
}
\description{
A dplyr src backed by SparkSQL, supports most operations other databases do with the familiar dplyr syntax and semantics.
}
\usage{
src_SparkSQL(
  host =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_BIND_HOST"),
     "localhost"),
  port =
    first.not.empty(
      Sys.getenv("HIVE_SERVER2_THRIFT_PORT"),
      10000),
  start.server = !is.server.running(),
  server.opts = list())
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{host}{
 The address of the host running the thift server (see Details)
}
  \item{port}{ The port on which the thirft server is listening}
 \item{start.server}{Whether to start the Spark thrift server, defaults to using one if already running, starting one otherwise.}
 \item{server.opts}{Server options, as a named list, will be added to command to start the Spark thrift server, see \url{http://spark.apache.org/docs/latest/sql-programming-guide.html#running-the-thrift-jdbcodbc-server}}
}

\details{
If you already have a thrift server running (see \url{https://spark.apache.org/docs/latest/sql-programming-guide.html}) you can set \code{start.server} to \code{NULL}. When \code{start.server} this function will try to guess if a server is already running, but that will work only if the server is local to the machine on which R is running.
}
\value{
An object of class \code{src_SparkSQL}. Use it like any other src of the SQL variety in dplyr.
}


\examples{
   #See the \code{dplyr} tutorial.
}
