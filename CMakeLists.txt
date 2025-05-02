cmake_minimum_required(VERSION 3.15)
project(Scramjet LANGUAGES C CXX)

# Use C++20 across the project
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(BUILD_SHARED_LIBS "Build shared libraries" ON)

# Attempt to find RocksDB via pkg-config
find_package(PkgConfig REQUIRED)
pkg_check_modules(PC_ROCKSDB rocksdb)
set(DISABLE_WARNING_AS_ERROR ON)

# set(DISABLE_WARNING_AS_ERROR ON)
if(PC_ROCKSDB_FOUND)
  message(STATUS "Found RocksDB via pkg-config: ${PC_ROCKSDB_INCLUDE_DIRS}, libs: ${PC_ROCKSDB_LIBRARIES}")
  set(ROCKSDB_INCLUDE_DIRS ${PC_ROCKSDB_INCLUDE_DIRS})
  set(ROCKSDB_LIBRARIES ${PC_ROCKSDB_LIBRARIES})
else()
  message(STATUS "RocksDB not found via pkg-config, fetching embedded vendor copy...")
  include(FetchContent)
  FetchContent_Declare(
    rocksdb
    GIT_REPOSITORY https://github.com/facebook/rocksdb.git
    GIT_TAG        "v10.2.1"
    GIT_SHALLOW    TRUE
  )
  FetchContent_MakeAvailable(rocksdb)
  set(ROCKSDB_LIBRARIES rocksdb)
  target_compile_options(rocksdb PRIVATE -Wno-unused-but-set-variable)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)

# Common include directories
add_subdirectory(src)
