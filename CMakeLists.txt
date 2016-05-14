cmake_minimum_required (VERSION 2.6)
project (libqueue)

set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wall -ggdb3")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Wall")
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS_RELEASE} -Wall -ggdb3")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

find_package(LevelDB REQUIRED)

include_directories(${LevelDB_INCLUDE} )

add_library(queue SHARED queue.c)


add_executable(qpush qpush.c ) 
target_link_libraries(qpush queue ${LevelDB_LIBRARIES} )

add_executable(qpop qpop.c queueutils.c) 
target_link_libraries(qpop queue ${LevelDB_LIBRARIES} )

add_executable(qpeek qpeek.c queueutils.c) 
target_link_libraries(qpeek queue ${LevelDB_LIBRARIES} )

add_executable(qpoke qpoke.c queueutils.c) 
target_link_libraries(qpoke queue ${LevelDB_LIBRARIES} )

add_executable(qlen qlen.c queueutils.c) 
target_link_libraries(qlen queue ${LevelDB_LIBRARIES} )

add_executable(qtest qtest.c queueutils.c) 
target_link_libraries(qtest queue ${LevelDB_LIBRARIES} )

install(TARGETS queue DESTINATION "lib")
install(FILES queue.h DESTINATION "include")
