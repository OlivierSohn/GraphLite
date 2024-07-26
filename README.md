# What is it?

An attempt to create a simple graph DB, in C++.

# Notes

```
brew install antlr
brew install antlr4-cpp-runtime
```

```
(base) Olivier@Oliviers-iMac dev % brew info antlr4-cpp-runtime   
==> antlr4-cpp-runtime: stable 4.13.1 (bottled)
ANother Tool for Language Recognition C++ Runtime Library
https://www.antlr.org/
Installed
/usr/local/Cellar/antlr4-cpp-runtime/4.13.1 (188 files, 3.2MB) *
```

To generate the cypher parser:
```
antlr -visitor -Dlanguage=Cpp -o ./graphdblite/src/cypherparser/ /Users/Olivier/Downloads/Cypher.g4
```