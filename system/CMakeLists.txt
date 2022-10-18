CMAKE_MINIMUM_REQUIRED (VERSION 3.10)
PROJECT(RTI CXX)

# Set default build type
IF(NOT CMAKE_BUILD_TYPE)
    MESSAGE(STATUS "Build type not set - defaulting to Release")
    SET(
        CMAKE_BUILD_TYPE "Release"
        CACHE
        STRING
        "Choose the type of build from: Debug Release RelWithDebInfo MinSizeRel Coverage."
        FORCE)
ENDIF()

# Use C++11 as standard
SET(CMAKE_CXX_STANDARD_REQUIRED ON)
SET(CMAKE_CXX_STANDARD 17)

SET (BOOST_MIN_VERSION "1.68.0")
set (Boost_NO_BOOST_CMAKE ON)

# Where to find cmake modules
SET(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake_modules")
INCLUDE(${CMAKE_MODULE_PATH}/FindPackageCustom.cmake)

message(STATUS "home path: ${CMAKE_SOURCE_DIR}")
###############################################################################
# Find necessary libraries and include dirs
###############################################################################
# Find boost headers and libs
FIND_PACKAGE(Boost REQUIRED COMPONENTS serialization system filesystem atomic thread)
IF(Boost_FOUND)
    SET(LIBS ${LIBS} ${Boost_LIBRARIES})
ELSE(Boost_FOUND)
    MESSAGE(FATAL_ERROR "Can't find boost libraries")
ENDIF()

# Find python
INCLUDE_DIRECTORIES(SYSTEM thirdparty/python3.7/include)

# Find ProtoBuf
INCLUDE_DIRECTORIES(SYSTEM thirdparty/protobuf-lib/include)
find_library(PROTOBUF_LIB NAMES protobuf PATHS ${CMAKE_SOURCE_DIR}/thirdparty/protobuf-lib/lib NO_DEFAULT_PATH)
SET(LIBS ${LIBS} ${PROTOBUF_LIB})
SET(PROTOC "${CMAKE_SOURCE_DIR}/thirdparty/protobuf-lib/bin/protoc")
MESSAGE(STATUS "protobuf lib: ${PROTOBUF_LIB}")
MESSAGE(STATUS "protoc bin: ${PROTOC}")
#IF CAMKE CANNOT FIND protobuf.so while it exists, run ldconfig command to clear cache

# Find gRPC
SET(GRPC_FIND_QUIETLY FALSE)
FIND_PACKAGE(gRPC REQUIRED)
INCLUDE_DIRECTORIES(SYSTEM ${GRPC_INCLUDE_DIR})
SET(LIBS ${LIBS} ${GRPC_LIBS})
SET(GRPC_CPP_PLUGIN "${CMAKE_SOURCE_DIR}/thirdparty/grpc/bin/grpc_cpp_plugin")
MESSAGE(STATUS "gRPC include dir: ${GRPC_INCLUDE_DIR}")
MESSAGE(STATUS "gRPC library path: ${GRPC_LIBS}")
MESSAGE(STATUS "gRPC compiler: ${GRPC_CPP_PLUGIN}")

# Add LibTorch if using torch c++
#FIND_PACKAGE(Torch REQUIRED)
#INCLUDE_DIRECTORIES(SYSTEM ${TORCH_INCLUDE_DIRS})
#SET(LIBS ${LIBS} ${TORCH_LIBRARIES})
#SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${TORCH_CXX_FLAGS}")
#set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3")

# Find GTest headers and libs
ADD_SUBDIRECTORY(thirdparty/googletest)
INCLUDE_DIRECTORIES(SYSTEM thirdparty/googletest/googletest/include)

# Find DLPack headers
INCLUDE_DIRECTORIES(SYSTEM thirdparty/dlpack/include)

# Find tensorboard_logger lib
INCLUDE_DIRECTORIES(SYSTEM thirdparty/tensorboard_logger/include)
FIND_LIBRARY(TENSORBOARD_LOGGER_LIB NAMES tensorboard_logger PATHS ${CMAKE_SOURCE_DIR}/thirdparty/tensorboard_logger/build NO_DEFAULT_PATH)
SET(LIBS ${LIBS} ${TENSORBOARD_LOGGER_LIB})

# System libs
FIND_LIBRARY(LIB_PTHREAD pthread)
FIND_LIBRARY(LIB_RT rt)
FIND_LIBRARY(LIB_DL dl)
FIND_LIBRARY(LIB_M m)
IF(LIB_PTHREAD AND LIB_RT AND LIB_DL AND LIB_M)
    SET(LIBS ${LIBS} ${LIB_PTHREAD} ${LIB_RT} ${LIB_DL} ${LIB_M})
ELSE()
    MESSAGE(FATAL_ERROR "Can't find system libraries\n  pthread: ${LIB_PTHREAD}\n  rt: ${LIB_RT}\n  dl: ${LIB_DL}\n  m: ${LIB_M}")
ENDIF()

# Find ttmath
FIND_PACKAGE_CUSTOM(ttmath "${CMAKE_SOURCE_DIR}/thirdparty/ttmath-0.9.3" "ttmath/ttmath.h" "" TRUE FALSE)

# Find Vtune
FIND_PACKAGE_CUSTOM(VTune "${CMAKE_SOURCE_DIR}/thirdparty/vtune" "ittnotify.h" "ittnotify" TRUE TRUE)

# Pybind11
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR}/thirdparty/pybind11/include)
# set(PYBIND11_CPP_STANDARD -std=c++14)
# Enable c++11 (a.k.a. c++0x)
if(UNIX)
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -Wall -msse4.2 -D_GNU_SOURCE -D__STDC_CONSTANT_MACROS -D__STDC_FORMAT_MACROS -D__STDC_LIMIT_MACROS -D__STRICT_ANSI__ -fPIC")
endif()

# Include dir
INCLUDE_DIRECTORIES(${CMAKE_SOURCE_DIR})
INCLUDE_DIRECTORIES("${CMAKE_CURRENT_SOURCE_DIR}/src")
INCLUDE_DIRECTORIES(${CMAKE_CURRENT_BINARY_DIR})

ADD_SUBDIRECTORY(src)
#ADD_SUBDIRECTORY(test)

SET(RTI_SRC
    src/rti/rti_server.cc
    )
ADD_EXECUTABLE(rti_engine ${RTI_SRC})
TARGET_LINK_LIBRARIES(rti_engine ${LIBS} storage interface)
set_property(TARGET rti_engine PROPERTY CXX_STANDARD 17)

SET(WRAPPER_SRC
    src/wrapper.cc
    )
ADD_LIBRARY(dan_wrapper SHARED ${WRAPPER_SRC})
TARGET_LINK_LIBRARIES(dan_wrapper PRIVATE storage)

SET(PYTHON_SRCS
    src/bind.cc
    )

ADD_SUBDIRECTORY(thirdparty/pybind11)
PYBIND11_ADD_MODULE(rti_python_lib MODULE ${PYTHON_SRCS})
TARGET_LINK_LIBRARIES(rti_python_lib PRIVATE storage interface)

###############################################################################
# Summary
###############################################################################
MESSAGE(STATUS "")
MESSAGE(STATUS "********** Summary **********")
MESSAGE(STATUS "General:")
MESSAGE(STATUS "  CMake version         : ${CMAKE_VERSION}")
MESSAGE(STATUS "  CMake command         : ${CMAKE_COMMAND}")
MESSAGE(STATUS "  System                : ${CMAKE_SYSTEM_NAME}")
MESSAGE(STATUS "  C++ compiler          : ${CMAKE_CXX_COMPILER}")
MESSAGE(STATUS "  C++ compiler version  : ${CMAKE_CXX_COMPILER_VERSION}")
MESSAGE(STATUS "  CXX flags             : ${CMAKE_CXX_FLAGS}")
MESSAGE(STATUS "  Build type            : ${CMAKE_BUILD_TYPE}")
MESSAGE(STATUS "  CMAKE_PREFIX_PATH     : ${CMAKE_PREFIX_PATH}")
MESSAGE(STATUS "  CMAKE_INSTALL_PREFIX  : ${CMAKE_INSTALL_PREFIX}")
MESSAGE(STATUS "")

get_property(ALL_INCLUDES DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR} PROPERTY INCLUDE_DIRECTORIES)
MESSAGE(STATUS "  All includes          : ${ALL_INCLUDES}")
MESSAGE(STATUS "  All libraries         : ${LIBS}")
MESSAGE(STATUS "")
