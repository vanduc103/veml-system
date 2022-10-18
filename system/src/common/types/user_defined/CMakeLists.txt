INCLUDE_DIRECTORIES(${RTI_SOURCE_DIR})

# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    tuple
    list
    )

SET(HDRS
  "types.h"
  "metadata.h"
  "tuple.h"
  "list.h"
  "list.inl"
  )

# Define library
ADD_LIBRARY(type_user_defined STATIC ${SRCS})

