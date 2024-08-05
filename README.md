# What is it?

An in-process Graph DB using SQLite.

The graph DB can be constructed via a C++ API and queried via `openCypher`
(only a subset of the `openCypher` grammar is supported at this point).

For an example usage, you can look at the code in [this file](src/main.cpp)

# Notes

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
