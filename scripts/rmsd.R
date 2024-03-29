#!/usr/bin/env Rscript

#Installing the necessary packages
#install.packages("bio3d", dependencies=TRUE)
#install.packages("iterpc", dependencies = TRUE)
#install.packages("tidyverse")
#install.packages("argparser", dependencies = TRUE)
#install.packages("bio3d", repos="http://cran.r-project.org", dependencies=TRUE, lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")
#install.packages("iterpc", repos="http://cran.r-project.org", dependencies=TRUE, lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")
#install.packages("tidyverse", repos="http://cran.r-project.org", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")

#Loading packages
library(bio3d) 
library(tidyverse)
library(iterpc)
library(parallel)
library(argparser)
#library("bio3d", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase") 
#library("tidyverse", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")
#library("iterpc", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")

# Handling commands in terminal using argparse
parser <- arg_parser(description = "RMSD calculation")
parser <- add_argument(parser, "--input", help = "PDB files path")
parser <- add_argument(parser, "--output", help = "Output file path")
parser <- add_argument(parser, "--threads", help = "Number of threads")
argsv <- parse_args(parser)

#Setting ncores
setup_core_value <- as.integer(argsv$threads)
setup.ncore(ncore=setup_core_value)

#Iterating over the pdb files for each family
#files <- list.files(path="/home/dan/CAZymes-MAN-XYL/parsing_pdb/GH115", pattern="\\.pdb", full.names = TRUE, recursive=FALSE)
files <- list.files(path=argsv$input, pattern="\\.pdb", full.names = TRUE, recursive=FALSE)

# Using iterpc package to obtain distinct combinations
J <- iterpc(table(files), 2, labels = files)

# Counting number of iterations
iterations <- nc_multiset(table(files), 2) # getlength(J) would work too
cat("Number of combinations:", iterations)

# Setting empty non-recursive vectors
ids1 <- character(0)
ids2 <- character(0)
return_rmsd <- double(0)

# Looping to get vectors
for (i in seq(iterations)) {
  combination <-  getnext(J)
  pdb1 <- read.pdb(combination[1], ATOM.only = TRUE, verbose = FALSE)
  name1 <- strsplit(combination[1], "/")
  name1_1 <- strsplit(name1[[1]][length(name1[[1]])], "\\.")
  name1_2 <- name1_1[[1]][1]
  ids1 <- c(ids1, name1_2)
  pdb2 <- read.pdb(combination[2], ATOM.only = TRUE, verbose = FALSE)
  name2 <- strsplit(combination[2], "/")
  name2_1 <- strsplit(name2[[1]][length(name1[[1]])], "\\.")
  name2_2 <- name2_1[[1]][1]
  ids2 <- c(ids2, name2_2)
  rmsd = rmsd(a=pdb1$xyz,b=pdb2$xyz, fit=TRUE,  ncore=setup_core_value, nseg.scale=setup_core_value) 
  return_rmsd <- c(return_rmsd, rmsd)
}

# Converting distance in similarity
max_distance_value <- max(return_rmsd)
min_distance_value <- min(return_rmsd)
rmsd_dataframe <- tibble(return_rmsd)
#similarity = tibble(apply(rmsd_dataframe, 1, function(x) {max_distance_value - x}))
similarity <- tibble(apply(rmsd_dataframe, 1, function(x) {(x - min_distance_value) / (max_distance_value - min_distance_value)}))

# Writing Edge List
data <- tibble(id1 = ids1, id2 = ids2, similarity)
write.table(data, file=argsv$output, sep="\t", row.names=FALSE, col.names = FALSE)