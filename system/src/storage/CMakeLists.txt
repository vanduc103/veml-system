# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    def_const
    catalog
    base/page
    base/segment
    base/segment_var
    base/segment_encoded
    base/paged_array
    base/paged_array_var
    relation/table
    relation/row_table
    graph/subgraph
    relation/column_table
    graph/graph
    graph/graph_delta
    graph/graph_main
    #graph/graph_training_data
    #graph/graph_conv_layer
    #graph/graph_sparse_training
    util/bit_vector
    util/page_allocator
    util/partition_func
    util/segment_map
    util/util_func
    util/data_transfer
    )

SET(HDRS
  "def_const.h"
  "catalog.h"
  "catalog.inl"
  "base/page.h"
  "base/segment.h"
  "base/segment_var.h"
  "base/segment_encoded.h"
  "base/segment_encoded.inl"
  "base/paged_array.h"
  "base/paged_array.inl"
  "base/paged_array_var.h"
  "base/paged_array_encoded.h"
  "base/paged_array_encoded.inl"
  "relation/table.h"
  "relation/row_table.h"
  "relation/column_table.h"
  "graph/graph.h"
  "graph/graph_delta.h"
  "graph/graph_main.h"
  "graph/graph_iterator.h"
  #"graph/graph_training_data.h"
  #"graph/graph_conv_layer.h"
  #"graph/graph_sparse_training.h"
  "util/btree.h"
  "util/btree.inl"
  "util/btree_index.h"
  "util/bit_vector.h"
  "util/page_allocator.h"
  "util/segment_map.h"
  "util/version_hash_map.h"
  "util/util_func.h"
  "util/partition_func.h"
  "util/data_transfer.h"
  "util/neural_ops.h"
  "util/py_dataloader.h"
  )

# Define library
ADD_LIBRARY(storage STATIC ${SRCS})
TARGET_LINK_LIBRARIES(storage types util concurrency ${LIBS})
