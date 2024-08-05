# What is it?

An in-process Graph DB using SQLite.

The Graph DB can be constructed via a C++ API (see [GraphDBSqlite.h](src/GraphDBSqlite.h)) and queried via `openCypher`
(see [CypherQuery.h](src/CypherQuery.h). Only a subset of the `openCypher` grammar is supported at this point).

For an example usage, you can look at the code in [main.cpp](src/main.cpp)

[PerformanceTests.cpp](src/PerformanceTests.cpp) contains benchmarks of some openCypher queries involving path patterns.

# Features summary

## Graph schema

### Nodes and edges ids

Nodes and edges of the graph are identified by an id.
Several id types are supported (`int64`, `double`, `text`, `blob`), but the type of the id has to be the same for all nodes and edges of the graph.

### Nodes and edges types

Nodes and edges have a type represented by a string homogeneous to an `openCypher` label.

### Nodes and edges properties

Each node or edge type has a given set of strongly typed properties. Supported property types are: `int64`, `double`, `text`, `blob`.

# Notes

## Antlr

Antlr is used for parsing the openCypher requests.

The `openCypher` Antlr parser code has been generated this way:

```
antlr -visitor -Dlanguage=Cpp -o ./graphdblite/src/cypherparser/ /Users/Olivier/Downloads/Cypher.g4
```

Antlr can be installed via `brew` on OSX:

```
brew install antlr
brew install antlr4-cpp-runtime
```

## Source code

I was initially planning to use Apache Parquet to persist the graph and Apache Arrow to load it into memory:
[GraphDB.cpp](src/GraphDB.cpp) and [GraphDB.h](src/GraphDB.h) are leftovers from this initial direction, but are not used to compile the project anymore.
