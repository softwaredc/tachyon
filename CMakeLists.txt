cmake_minimum_required(VERSION 3.16)
project(tachyon VERSION 1.0.0 LANGUAGES C)

# ----------------------------------------------------------------------------
# tachyon has no dependencies. Nothing outside libc is needed to build, run
# tests, or benchmark, apart from pthreads and rt, which ship with every Linux
# distribution. C11 atomics are required, so a pre-4.9 gcc will not work.
# ----------------------------------------------------------------------------

set(CMAKE_C_STANDARD 11)
set(CMAKE_C_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(TACHYON_BUILD_TESTS    "Build the test suite"        ON)
option(TACHYON_BUILD_BENCH    "Build benchmarks"            ON)
option(TACHYON_BUILD_EXAMPLES "Build examples"              ON)
option(TACHYON_BUILD_TOOLS    "Build tachyond and tyctl"    ON)
option(TACHYON_BINDINGS_PY    "Install the Python binding"  ON)

set(TACHYON_WARNING_FLAGS -Wall -Wextra -Wshadow -Wconversion
                          -Wsign-conversion -Wundef -Wpointer-arith)

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
  add_compile_options(-g3 -O0 -DTACHYON_DEBUG=1)
elseif(CMAKE_BUILD_TYPE STREQUAL "RelWithDebInfo")
  add_compile_options(-g -O2)
else()
  # -fno-plt keeps indirect calls direct, worth a measurable amount on the
  # benchmarking targets and harmless elsewhere.
  add_compile_options(-O3 -fomit-frame-pointer -DNDEBUG)
endif()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/src)

set(TACHYON_SOURCES
  src/util.c
  src/ring.c
  src/notify.c
  src/pool.c
  src/reaper.c
  src/topic.c
  src/shm.c
)

add_library(tachyon_obj OBJECT ${TACHYON_SOURCES})
target_compile_options(tachyon_obj PRIVATE ${TACHYON_WARNING_FLAGS})

add_library(tachyon SHARED $<TARGET_OBJECTS:tachyon_obj>)
target_compile_definitions(tachyon PRIVATE TACHYON_EXPORTS)
set_target_properties(tachyon PROPERTIES
  VERSION   ${CMAKE_PROJECT_VERSION}
  SOVERSION 1
  OUTPUT_NAME tachyon
  PUBLIC_HEADER include/tachyon/tachyon.h
)
target_link_libraries(tachyon PRIVATE pthread rt)

add_library(tachyon_static STATIC $<TARGET_OBJECTS:tachyon_obj>)
set_target_properties(tachyon_static PROPERTIES OUTPUT_NAME tachyon)
target_link_libraries(tachyon_static PUBLIC pthread rt)

# Must come before any add_subdirectory that registers tests: add_test() only
# takes effect when testing is already enabled for that directory scope.
if(TACHYON_BUILD_TESTS)
  enable_testing()
endif()

if(TACHYON_BUILD_EXAMPLES)
  add_subdirectory(examples)
endif()
if(TACHYON_BUILD_TESTS)
  add_subdirectory(tests)
endif()
if(TACHYON_BUILD_BENCH)
  add_subdirectory(benchmarks)
endif()
if(TACHYON_BUILD_TOOLS)
  add_subdirectory(tools)
endif()

install(TARGETS tachyon tachyon_static
        LIBRARY  DESTINATION lib
        ARCHIVE  DESTINATION lib)
install(FILES include/tachyon/tachyon.h DESTINATION include/tachyon)

# Summarise the configuration, which is genuinely useful when someone files a
# bug report: the build matrix is visible in one block.
message(STATUS "")
message(STATUS "  tachyon ${CMAKE_PROJECT_VERSION}")
message(STATUS "  build type        : ${CMAKE_BUILD_TYPE}")
message(STATUS "  C compiler        : ${CMAKE_C_COMPILER_ID} ${CMAKE_C_COMPILER_VERSION}")
message(STATUS "  system            : ${CMAKE_SYSTEM_NAME} ${CMAKE_SYSTEM_PROCESSOR}")
message(STATUS "  tests             : ${TACHYON_BUILD_TESTS}")
message(STATUS "  benchmarks        : ${TACHYON_BUILD_BENCH}")
message(STATUS "  tools             : ${TACHYON_BUILD_TOOLS}")
message(STATUS "")
