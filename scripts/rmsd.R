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
library(tibble)
library(iterpc)
#library(argparser)
#library("bio3d", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase") 
#library("tibble", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")
#library("iterpc", lib="/Storage/data2/danilo.brito/CAZymes-MAN-XYL/DB/XylanDatabase")

#Iterating over the pdb files for each family
files <- list.files(path="/home/dan/CAZymes-MAN-XYL/grupos/alphafold_gh115", pattern="\\.pdb", full.names = TRUE, recursive=FALSE)

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
  pdb1 <- read.pdb(combination[1], ATOM.only = TRUE)
  name1 <- strsplit(combination[1], "/")
  name1.1 <- strsplit(name1[[1]][length(name1[[1]])], "\\.")
  name1.2 <- name1.1[[1]][1]
  ids1 <- c(ids1, name1.2)
  pdb2 <- read.pdb(combination[2], ATOM.only = TRUE)
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

write.table(data, file = "GH115_atomonly.csv", sep="\t", row.names=FALSE, col.names = FALSE)