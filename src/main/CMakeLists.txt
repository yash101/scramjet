add_executable(scramjet
  scramjet.cpp
)

target_link_libraries(scramjet PRIVATE ${ROCKSDB_LIBRARIES})
target_include_directories(scramjet PRIVATE ${ROCKSDB_INCLUDE_DIRS})
