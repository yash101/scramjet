cmake_minimum_required(VERSION 3.15)
project(Scramjet LANGUAGES C CXX)

# Use C++20 across the project
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

# Attempt to find LMDB via pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_LMDB lmdb)

if(PC_LMDB_FOUND)
  message(STATUS "Found LMDB via pkg-config: ${PC_LMDB_INCLUDE_DIRS}, libs: ${PC_LMDB_LIBRARIES}")
  set(LMDB_INCLUDE_DIRS ${PC_LMDB_INCLUDE_DIRS})
  set(LMDB_LIBRARIES ${PC_LMDB_LIBRARIES})
else()
  message(STATUS "LMDB not found via pkg-config, fetching embedded vendor copy...")
  include(FetchContent)
  FetchContent_Declare(
    lmdb
    GIT_REPOSITORY https://github.com/LMDB/lmdb.git
    GIT_TAG        "LMDB_0.9.31"
  )
  FetchContent_MakeAvailable(lmdb)
  # lmdb tree places sources under libraries/liblmdb
  set(LMDB_INCLUDE_DIRS ${lmdb_SOURCE_DIR}/libraries/liblmdb)
  add_library(lmdb_vendor STATIC
    ${lmdb_SOURCE_DIR}/libraries/liblmdb/mdb.c
  )
  target_include_directories(lmdb_vendor PUBLIC ${LMDB_INCLUDE_DIRS})
  set(LMDB_LIBRARIES lmdb_vendor)
endif()

# Common include directories
include_directories(${LMDB_INCLUDE_DIRS})

add_subdirectory(src)
