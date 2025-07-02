cmake_minimum_required(VERSION 3.10)
project(LimboDB)

set(CMAKE_CXX_STANDARD 20)

# Include directories
include_directories(
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/external/pretty
)

# Source files
set(SOURCES
    main.cpp
    src/disk_manager.cpp
    src/record_iterator.cpp
    src/record_manager.cpp
    src/catalog_manager.cpp
    src/table_manager.cpp
    src/index_manager.cpp
    src/query/query_parser.cpp
    external/pretty/pretty.cpp   # Implementation
    # src/btree.cpp
)

# Executable
add_executable(dbms ${SOURCES})
