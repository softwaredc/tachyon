# Example programs. Kept small on purpose: these are the snippets people copy
# into their own projects, so they must compile cleanly and read top to bottom.

set(TACHYON_EXAMPLES
    hello_ipc
    pingpong
    multi_partition
    key_ordering
    zero_copy_payload
    crash_recovery
)

foreach(src ${TACHYON_EXAMPLES})
    if(EXISTS ${CMAKE_CURRENT_SOURCE_DIR}/${src}.c)
        add_executable(${src} ${src}.c)
        target_link_libraries(${src} PRIVATE tachyon_static pthread rt)
        target_compile_options(${src} PRIVATE -Wall -Wextra)
    endif()
endforeach()

# key_ordering and crash_recovery are assertions, not demos: they exit non-zero
# when the invariant they check is broken. Worth running in CI.
if(TARGET key_ordering)
    add_test(NAME example_key_ordering COMMAND key_ordering)
    set_tests_properties(example_key_ordering PROPERTIES TIMEOUT 90)
endif()
if(TARGET crash_recovery)
    add_test(NAME example_crash_recovery COMMAND crash_recovery)
    set_tests_properties(example_crash_recovery PROPERTIES TIMEOUT 90)
endif()
