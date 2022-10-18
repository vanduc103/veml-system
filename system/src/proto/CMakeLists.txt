# Set files to create library
# If you add new source file(s), add file's name to following lines

###########################################################
# Compile 'interface.proto'
get_filename_component(proto_file "./interface.proto" ABSOLUTE)
get_filename_component(proto_path "${proto_file}" PATH)

# generate target dir
set(target_dir "${CMAKE_SOURCE_DIR}/interface")

# set output file
set(proto_srcs "${target_dir}/interface.pb.cc")
set(proto_hdrs "${target_dir}/interface.pb.h")
set(grpc_srcs "${target_dir}/interface.grpc.pb.cc")
set(grpc_hdrs "${target_dir}/interface.grpc.pb.h")
set(proto_py "${target_dir}/interface_pb2.py")
set(grpc_py "${target_dir}/interface_pb2_grpc.py")

add_custom_command(
    OUTPUT "${proto_srcs}" "${proto_hdrs}" "${grpc_srcs}" "${grpc_hdrs}"
    COMMAND ${PROTOC}
    ARGS --grpc_out "${target_dir}"
        --cpp_out "${target_dir}"
        -I "${proto_path}"
        --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
        "${proto_file}"
    DEPENDS "${proto_file}"
    COMMENT "Generating grpc cpp interface for ${proto_file}"
    )

# include generated *.pb.h files
include_directories("${target_dir}")

###########################################################
# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
  ${proto_srcs}
  ${grpc_srcs}
  )

ADD_LIBRARY(interface ${SRCS})
