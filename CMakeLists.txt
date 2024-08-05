cmake_minimum_required(VERSION 3.5)

project(GraphDBLite)


#set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} “/Users/Olivier/Dev/vcpkg/ports/czmq/”)

#set(CMAKE_BUILD_TYPE Debug)
set(CMAKE_BUILD_TYPE Release)
# To have debug symbols in XCode, may need to remove this for release build.
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

# find_package(Arrow CONFIG REQUIRED) 
# find_package(Parquet CONFIG REQUIRED)

#find_package(Arrow REQUIRED)

# From https://google.github.io/googletest/quickstart-cmake.html

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
  DOWNLOAD_EXTRACT_TIMESTAMP True
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

add_executable(
  Tests
  src/TestUtils.cpp
  src/TestUtils.inl
  src/TestUtils.h
  src/Tests.cpp
  src/PerformanceTests.cpp
)

include(GoogleTest)
gtest_discover_tests(Tests)

# For statically linking a sqlidb extension, see https://www.sqlite.org/loadext.html
add_compile_definitions(SQLITE_CORE)

add_library(
  GraphDBLiteLib
  SHARED
  src/sqlext/carray.c
  src/sqlext/carray.h
  src/Metaprog.h
  src/SQLPreparedStatement.cpp
  src/SQLPreparedStatement.h
  src/Value.cpp
  src/Value.h
  src/MyCypherVisitor.cpp
  src/MyCypherVisitor.h
  src/SqlAST.h
  src/CypherAST.h
  src/CypherQuery.cpp
  src/CypherQuery.inl
  src/CypherQuery.h
  src/Cypher.g4
  src/GraphDBSqlite.cpp
  src/GraphDBSqlite.h
  src/GraphDBSqliteTypes.h
  src/Logs.h
  src/Logs.cpp
  src/cypherparser/CypherBaseListener.cpp
  src/cypherparser/CypherBaseListener.h
  src/cypherparser/CypherBaseVisitor.cpp
  src/cypherparser/CypherBaseVisitor.h
  src/cypherparser/CypherLexer.cpp
  src/cypherparser/CypherLexer.h
  src/cypherparser/CypherParser.h
  src/cypherparser/CypherListener.cpp
  src/cypherparser/CypherListener.h
  src/cypherparser/CypherParser.cpp
  src/cypherparser/CypherVisitor.h
  src/cypherparser/CypherVisitor.cpp
)

add_executable(
  GraphDBLite
  src/main.cpp
)

target_include_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/antlr4-cpp-runtime/4.13.1/include/antlr4-runtime/
)

# Apache Arrow is not needed anymore
target_include_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/apache-arrow/16.1.0_3/include
)

target_include_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/sqlite/3.46.0/include
)

target_link_libraries(
  GraphDBLiteLib
  PRIVATE
  antlr4-runtime
  sqlite3
#  parquet
#  arrow
)

# target_link_libraries(GraphDBLite PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Arrow::arrow_static,Arrow::arrow_shared>")
# target_link_libraries(GraphDBLite PRIVATE "$<IF:$<BOOL:${ARROW_BUILD_STATIC}>,Parquet::parquet_static,Parquet::parquet_shared>")

#target_link_libraries(
#  GraphDBLite
#  PRIVATE
#  Arrow::arrow_shared)

target_link_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/antlr4-cpp-runtime/4.13.1/lib/
)
target_link_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/apache-arrow/16.1.0_3/lib/
)
target_link_directories(
  GraphDBLiteLib
  PRIVATE
  /usr/local/Cellar/sqlite/3.46.0/lib/
)

target_compile_features(GraphDBLiteLib PRIVATE cxx_std_20)
target_compile_features(GraphDBLite PRIVATE cxx_std_20)
target_compile_features(Tests PRIVATE cxx_std_20)

target_link_libraries(
  Tests
  PRIVATE
  GraphDBLiteLib
  GTest::gtest_main
)
target_link_libraries(
  GraphDBLite
  PRIVATE
  GraphDBLiteLib
)
