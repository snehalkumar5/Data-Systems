# Data Systems

Project to extend a data system codebase to include additional features.

Features added:
1. Support for matrix tables
2. Support for sparse matrix tables using compression algorithms
3. Join algorithms - Block nested, Partition Hash
4. Group By aggregates
5. Two phase merge sorting

## Instructions
```
cd src
```
To compile
```
make clean
make
```

## To run

Post compilation, an executable names ```server``` will be created in the ```src``` directory
```
./server
```
