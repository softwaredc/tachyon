# Test suite. No external framework -- the library has zero dependencies and
# the tests inherit that constraint, so `make check` works anywhere a C
# compiler and libc exist.

set(TACHYON_TEST_LIBS tachyon_static pthread rt)

foreach(t test_basic test_crash test_chaos test_stress)
    add_executable(${t} ${t}.c)
    target_link_libraries(${t} PRIVATE ${TACHYON_TEST_LIBS})
    target_include_directories(${t} PRIVATE ${CMAKE_SOURCE_DIR}/src)
    target_compile_options(${t} PRIVATE -Wall -Wextra)
endforeach()

# test_basic and test_stress are single-process, so they are safe under TSan.
# test_crash and test_chaos fork and kill children on purpose, which no
# sanitizer understands, so they run plain.
add_test(NAME basic  COMMAND test_basic)
add_test(NAME crash  COMMAND test_crash)
add_test(NAME stress COMMAND test_stress)
