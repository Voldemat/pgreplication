cmake_minimum_required(VERSION 4.1.0)
project(pgreplication VERSION 0.0.1 LANGUAGES CXX)

if (NOT DEFINED PGREPLICATION_NAMESPACE)
    set(PGREPLICATION_NAMESPACE pgreplication)
endif()

if (NOT DEFINED PGREPLICATION_ADD_STD_VARIANT_FORMATTER)
    set(PGREPLICATION_ADD_STD_VARIANT_FORMATTER ON)
endif()

file(GLOB_RECURSE PGREPLICATION_SOURCES src/*.cpp src/*.c)
add_library(pgreplication_object OBJECT ${PGREPLICATION_SOURCES})
set_target_properties(pgreplication_object PROPERTIES
    CXX_STANDARD 26
    C_EXTENSIONS OFF
    CXX_EXTENSIONS OFF
    CXX_STANDARD_REQUIRED ON
)

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
    set_target_properties(pgreplication_static PROPERTIES
        CXX_STANDARD 26
        C_EXTENSIONS OFF
        CXX_EXTENSIONS OFF
        CXX_STANDARD_REQUIRED ON
    )
endif()
if (PGREPLICATION_SHARED)
    add_library(pgreplication_shared SHARED $<TARGET_OBJECTS:pgreplication_object>)
    target_compile_options(pgreplication_shared PUBLIC -DPGREPLICATION_NAMESPACE=${PGREPLICATION_NAMESPACE} -DPGREPLICATION_ADD_STD_VARIANT_FORMATTER=${PGREPLICATION_ADD_STD_VARIANT_FORMATTER})
    target_include_directories(
        pgreplication_shared
        PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src/
    )
    set_target_properties(pgreplication_shared PROPERTIES
        CXX_STANDARD 26
        C_EXTENSIONS OFF
        CXX_EXTENSIONS OFF
        CXX_STANDARD_REQUIRED ON
    )
endif()

if (PGREPLICATION_TESTS)
    include("${CMAKE_CURRENT_SOURCE_DIR}/CMakeTestLists.txt")
endif()
