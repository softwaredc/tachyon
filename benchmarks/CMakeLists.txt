add_executable(bench_ipc bench_ipc.c)
target_link_libraries(bench_ipc PRIVATE tachyon_static pthread rt)
target_include_directories(bench_ipc PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_compile_options(bench_ipc PRIVATE -Wall -Wextra)

# TY_NOTIFY=spin|futex|hybrid selects the wait policy at runtime, so one binary
# produces the whole comparison table.
add_custom_target(bench
    COMMAND ${CMAKE_COMMAND} -E echo "run scripts/run_benchmarks.sh for the full matrix"
    DEPENDS bench_ipc)
