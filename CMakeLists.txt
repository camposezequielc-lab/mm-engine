cmake_minimum_required(VERSION 3.21)
project(mm_engine CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

# Dependencies via vcpkg manifest (vcpkg.json): quickfix, lmdb, prometheus-cpp
find_package(quickfix CONFIG QUIET)
if(NOT quickfix_FOUND)
    find_library(QUICKFIX_LIB quickfix REQUIRED)
    find_path(QUICKFIX_INCLUDE quickfix/Application.h REQUIRED)
endif()
find_package(prometheus-cpp CONFIG REQUIRED)
find_package(unofficial-lmdb CONFIG QUIET)
if(NOT unofficial-lmdb_FOUND)
    find_library(LMDB_LIB lmdb REQUIRED)
    find_path(LMDB_INCLUDE lmdb.h REQUIRED)
endif()
find_package(Threads REQUIRED)

add_executable(mm_engine src/main.cpp)

target_include_directories(mm_engine PRIVATE src)

if(MSVC)
    # /O2 max speed, /Ob3 aggressive inline, /arch:AVX2 enables the SIMD path,
    # /GL+/LTCG whole-program optimization, /Zi PDBs so the profiler can symbolize.
    target_compile_options(mm_engine PRIVATE /O2 /Ob3 /arch:AVX2 /GL /Zi /EHsc /W4 /wd4100)
    target_link_options(mm_engine PRIVATE /LTCG /DEBUG)
    target_compile_definitions(mm_engine PRIVATE NOMINMAX WIN32_LEAN_AND_MEAN _CRT_SECURE_NO_WARNINGS)
else()
    target_compile_options(mm_engine PRIVATE -O3 -march=native -fno-omit-frame-pointer -g -Wall)
endif()

if(quickfix_FOUND)
    target_link_libraries(mm_engine PRIVATE quickfix)
else()
    target_include_directories(mm_engine PRIVATE ${QUICKFIX_INCLUDE})
    target_link_libraries(mm_engine PRIVATE ${QUICKFIX_LIB})
endif()

if(unofficial-lmdb_FOUND)
    target_link_libraries(mm_engine PRIVATE unofficial::lmdb::lmdb)
else()
    target_include_directories(mm_engine PRIVATE ${LMDB_INCLUDE})
    target_link_libraries(mm_engine PRIVATE ${LMDB_LIB})
endif()

target_link_libraries(mm_engine PRIVATE
    prometheus-cpp::pull
    Threads::Threads)

if(WIN32)
    target_link_libraries(mm_engine PRIVATE ws2_32)
endif()

# copy runtime config next to the binary
add_custom_command(TARGET mm_engine POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_SOURCE_DIR}/config $<TARGET_FILE_DIR:mm_engine>/config)
