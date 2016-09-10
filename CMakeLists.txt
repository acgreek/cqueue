cmake_minimum_required (VERSION 2.6)
project (cqueue)

find_package(PkgConfig)
pkg_check_modules (EXTREMECUNIT ExtremeCUnit)

string(REPLACE ";" " " EXTREMECUNIT_LDFLAGS2 "${EXTREMECUNIT_LDFLAGS}" )
string(REPLACE ";" " " EXTREMECUNIT_CFLAGS2 "${EXTREMECUNIT_CFLAGS}" )
message(STATUS "CFLAGS=" ${EXTREMECUNIT_CFLAGS2})

set(CMAKE_COMPILE_COMMON_FLAGS "-O0 -D_GNU_SOURCE -std=c99 -Wall -ggdb3 ")
set(CMAKE_C_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} ${CMAKE_COMPILE_COMMON_FLAGS} -ggdb3")
set(CMAKE_C_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_COMPILE_COMMON_FLAGS}")
set(CMAKE_C_FLAGS "${CMAKE_CXX_FLAGS_RELEASE} ${CMAKE_COMPILE_COMMON_FLAGS}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")


add_library(queue SHARED queue.c)
add_library(queueStatic STATIC queue.c)
set_target_properties(queueStatic PROPERTIES OUTPUT_NAME queue)

set(MYLIB_VERSION_MAJOR 1)
set(MYLIB_VERSION_MINOR 2)
set(MYLIB_VERSION_PATCH 0)

set(MYLIB_VERSION_STRING ${MYLIB_VERSION_MAJOR}.${MYLIB_VERSION_MINOR}.${MYLIB_VERSION_PATCH})

set_target_properties(queue PROPERTIES VERSION ${MYLIB_VERSION_STRING} SOVERSION ${MYLIB_VERSION_MAJOR})

add_executable(bench bench.c ) 
target_link_libraries(bench queue)

add_executable(qpush qpush.c ) 
target_link_libraries(qpush queue)

add_executable(qpop qpop.c queueutils.c) 
target_link_libraries(qpop queue)

add_executable(qpeek qpeek.c queueutils.c) 
target_link_libraries(qpeek queue)

add_executable(qpoke qpoke.c queueutils.c) 
target_link_libraries(qpoke queue)

add_executable(qlen qlen.c queueutils.c) 
target_link_libraries(qlen queue)

add_executable(qcount qcount.c queueutils.c) 
target_link_libraries(qcount queue)

add_executable(qcompact qcompact.c queueutils.c) 
target_link_libraries(qcompact queue)

add_executable(qrepair qrepair.c queueutils.c) 
target_link_libraries(qrepair queue)

add_executable(qtest qtest.c queueutils.c) 
target_link_libraries(qtest queue)

add_executable(qtest1 qtest1.c queueutils.c) 
SET_TARGET_PROPERTIES(qtest1 PROPERTIES
      COMPILE_FLAGS "-ggdb ${EXTREMECUNIT_CFLAGS2}"
)

target_link_libraries(qtest1 ${EXTREMECUNIT_LDFLAGS})


install(TARGETS queue DESTINATION "lib")
install(FILES queue.h DESTINATION "include")

add_custom_target(test qtest qtest)

