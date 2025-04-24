add_executable(scramjet
  scramjet.cpp
)

target_link_libraries(scramjet PRIVATE ${LMDB_LIBRARIES})
target_include_directories(scramjet PRIVATE ${LMDB_INCLUDE_DIRS})
