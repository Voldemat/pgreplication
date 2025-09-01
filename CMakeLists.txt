cmake_minimum_required(VERSION 4.1.0)
project(pgreplication VERSION 0.0.1 LANGUAGES C CXX)
mark_as_advanced(CMAKE_MAKE_PROGRAM)
include(GNUInstallDirs)
include(CMakePrintHelpers)
include(FetchContent)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_C_EXTENSIONS OFF)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (CMAKE_CURRENT_SOURCE_DIR STREQUAL CMAKE_SOURCE_DIR)
    set(IS_ROOT_PROJECT ON)
else()
    set(IS_ROOT_PROJECT OFF)
endif()

if (NOT DEFINED PGREPLICATION_NAMESPACE)
    set(PGREPLICATION_NAMESPACE pgreplication)
endif()

if (NOT DEFINED PGREPLICATION_ADD_STD_VARIANT_FORMATTER)
    set(PGREPLICATION_ADD_STD_VARIANT_FORMATTER ON)
endif()

file(GLOB_RECURSE PGREPLICATION_SOURCES src/*.cpp src/*.c)
add_library(pgreplication_object OBJECT ${PGREPLICATION_SOURCES})
set_property(TARGET pgreplication_object PROPERTY POSITION_INDEPENDENT_CODE 1)
target_compile_options(pgreplication_object PUBLIC -DPGREPLICATION_NAMESPACE=${PGREPLICATION_NAMESPACE} -DPGREPLICATION_ADD_STD_VARIANT_FORMATTER=${PGREPLICATION_ADD_STD_VARIANT_FORMATTER})
target_include_directories(
    pgreplication_object
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/
)
if (PGREPLICATION_TESTS OR PGREPLICATION_STATIC)
    add_library(pgreplication_static STATIC $<TARGET_OBJECTS:pgreplication_object>)
    target_compile_options(pgreplication_static PUBLIC -DPGREPLICATION_NAMESPACE=${PGREPLICATION_NAMESPACE} -DPGREPLICATION_ADD_STD_VARIANT_FORMATTER=${PGREPLICATION_ADD_STD_VARIANT_FORMATTER})
    target_include_directories(
        pgreplication_static
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/
    )
endif()
if (PGREPLICATION_SHARED)
    add_library(pgreplication_shared SHARED $<TARGET_OBJECTS:pgreplication_object>)
    target_compile_options(pgreplication_shared PUBLIC -DPGREPLICATION_NAMESPACE=${PGREPLICATION_NAMESPACE} -DPGREPLICATION_ADD_STD_VARIANT_FORMATTER=${PGREPLICATION_ADD_STD_VARIANT_FORMATTER})
    target_include_directories(
        pgreplication_shared
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/
    )
endif()

if (PGREPLICATION_TESTS)
    include("${CMAKE_CURRENT_SOURCE_DIR}/CMakeTestLists.txt")
endif()
