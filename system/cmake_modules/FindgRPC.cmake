# - Find gRPC
# This module defines
#  GRPC_INCLUDE_DIR, where to find THRIFT headers
#  GRPC_LIBS, GRPC libraries
#  GRPC_FOUND, If false, do not try to use ant

find_path(GRPC_INCLUDE_DIR grpc/grpc.h PATHS ${CMAKE_SOURCE_DIR}/thirdparty/grpc/include NO_DEFAULT_PATH)
find_library(GRPC_LIB NAMES grpc PATHS ${CMAKE_SOURCE_DIR}/thirdparty/grpc/lib NO_DEFAULT_PATH)
find_library(GRPCPP_LIB NAMES grpc++ PATHS ${CMAKE_SOURCE_DIR}/thirdparty/grpc/lib NO_DEFAULT_PATH)

if (GRPC_LIB)
  set(GRPC_FOUND TRUE)
  set(GRPC_LIBS ${GRPC_LIB} ${GRPCPP_LIB})
else ()
  set(GRPC_FOUND FALSE)
endif ()

if (NOT GRPC_FOUND)
  if (NOT GRPC_FIND_QUIETLY)
    message(STATUS "gRPC compiler/libraries NOT found. "
      "gRPC support will be disabled (${GRPC_RETURN}, "
      "${GRPC_INCLUDE_DIR}, ${GRPC_LIB})")
  endif ()
endif ()

mark_as_advanced(
  GRPC_LIB
  GRPC_COMPILER
  GRPC_INCLUDE_DIR
)
