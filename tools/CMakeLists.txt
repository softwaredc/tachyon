# Operational tooling. Small on purpose: these exist to answer "why is it
# slow" and "who died", not to become a management platform.

add_executable(tachyond tachyond.c)
target_link_libraries(tachyond PRIVATE tachyon_static pthread rt)
target_include_directories(tachyond PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_compile_options(tachyond PRIVATE -Wall -Wextra)

add_executable(tyctl tyctl.c)
target_link_libraries(tyctl PRIVATE tachyon_static pthread rt)
target_include_directories(tyctl PRIVATE ${CMAKE_SOURCE_DIR}/src)
target_compile_options(tyctl PRIVATE -Wall -Wextra)
