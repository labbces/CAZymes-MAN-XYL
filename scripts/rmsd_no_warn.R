#!/usr/bin/env Rscript

#Installing the necessary packages
install.packages("bio3d", dependencies=TRUE)
install.packages("iterpc", dependencies = TRUE)
install.packages("tidyverse")
#install.packages("argparser", dependencies = TRUE)
#install.packages("iterators")
#install.packages("foreach")
#install.packages("doParallel")

# Removing undesired warnings
oldw <- getOption("warn")
options(warn = -1)

#Loading packages
library(bio3d) 
library(tibble)
library(iterpc)
#library(argparser)
#library(iterators)
#library(foreach)
#library(parallel)
#library(doParallel)



#Iterating over the pdb files for each family
files <- list.files(path="/home/dan/CAZymes-MAN-XYL/grupos/alphafold_gh115", pattern="\\.pdb", full.names = TRUE, recursive=FALSE)
#files2 <- list.files(path="/home/dan/CAZymes-MAN-XYL/parsing_pdb/GH95", pattern="\\.pdb", recursive=FALSE)

# Using iterpc package to obtain distinct combinations
J <- iterpc(table(files), 2, labels = files)

# Counting number of iterations
iterations <- nc_multiset(table(files), 2) # getlength(J) would work too
print(iterations)

# Setting empty non-recursive vectors
ids1 <- character(0)
ids2 <- character(0)
similarity <- double(0)

# Looping to get vectors
for(i in seq(iterations)) {
  combination <-  getnext(J)
  pdb1 <- read.pdb(combination[1])
  name1 <- strsplit(combination[1], "/")
  name1.1 <- strsplit(name1[[1]][length(name1[[1]])], "\\.")
  name1.2 <- name1.1[[1]][1]
  ids1 <- c(ids1, name1.2)
  pdb2 <- read.pdb(combination[2])
  name2 <- strsplit(combination[2], "/")
  name2.1 <- strsplit(name2[[1]][length(name1[[1]])], "\\.")
  name2.2 <- name2.1[[1]][1]
  ids2 <- c(ids2, name2.2)
  return_rmsd = rmsd(a=pdb1$xyz,b=pdb2$xyz, fit=TRUE) 
  return_similarity = return_rmsd * -1
  #cat(name1.2, name2.2, return_rmsd, return_similarity,"\n")
  similarity <- c(similarity, return_similarity)
}

# Writing Edge List
data = tibble(id1 = ids1, id2 = ids2, similarity = similarity)

write.table(data, file = "GH31.csv", sep="\t", row.names=FALSE, col.names = FALSE)

on.exit(options(warn = oldw))

#foreach(x=it) %do% {pdb1 = read.pdb(x[1]); pdb2 = read.pdb(x[2])} %do% #{ rmsd(a=x[1]$xyz, b=x[2]$xyz, fit=TRUE) }
#foreach(x=it) %do% {print(x[2])}

