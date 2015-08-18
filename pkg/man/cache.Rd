\name{cache}
\alias{cache}

\title{
Cache a table into distributed memory
}
\description{
One of the peculiar features of spark is the ability to temporarily store data into distributed memory for faster access. This is particularly useful when accessing the same table multiple times. This also will change the data format to an internal, optimized spark format for additional performance benefits. When RAM size is exceeded, data is spilled to disk and performance degrades gradually.
}
\usage{
cache(tbl)
}
%- maybe also 'usage' for other objects documented here.
\arguments{
  \item{tbl}{A table of class \code{tbl_SparkSQL}
}
}
\value{
The same table passed as the only argument. This function works by side effects and the return value can be safely discarded.
}
