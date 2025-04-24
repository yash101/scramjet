enable_testing()
add_executable(tsdb_test
  test.cpp
)

target_link_libraries(tsdb_test PRIVATE ${LMDB_LIBRARIES})
target_include_directories(tsdb_test PRIVATE ${LMDB_INCLUDE_DIRS})

add_test(NAME TSDBTest COMMAND tsdb_test)

