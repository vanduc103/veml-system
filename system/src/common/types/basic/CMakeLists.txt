INCLUDE_DIRECTORIES(${RTI_SOURCE_DIR})

# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    types
    date
    )

SET(HDRS
  "numeric.h"
  "bool.h"
  "date.h"
  "time.h"
  "timestamp.h"
  "decimal.h"
  "string.h"
  "types.h"
  )

# Define library
ADD_LIBRARY(type_basic STATIC ${SRCS})
