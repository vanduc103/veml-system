# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    archive_manager
    log_manager
    )

SET(HDRS
  "log_manager.h"
  "archive_managet.h"
  )

# Define library
ADD_LIBRARY(persistency STATIC ${SRCS})
TARGET_LINK_LIBRARIES(persistency ${LIBS})
