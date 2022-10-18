INCLUDE_DIRECTORIES(${RTI_SOURCE_DIR})

# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    def_const
    element_type
    eval_type
    types
    )

SET(HDRS
  "def_const.h"
  "element_type.h"
  "eval_type.h"
  "hash_util.h"
  "types.h"
  )

SET(DEPEND_LIBS
  type_basic
  type_user_defined
  )
  
# Set sub-directories to create library
ADD_SUBDIRECTORY(basic)
ADD_SUBDIRECTORY(user_defined)

# Define library
ADD_LIBRARY(types STATIC ${SRCS})
TARGET_LINK_LIBRARIES(types ${DEPEND_LIBS})
