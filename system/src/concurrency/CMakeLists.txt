# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    transaction
    version
    version_map
    )

SET(HDRS
    "mutex_lock.h"
    "rti_thread.h"
    "transaction.h"
    "version.h"
    "version_map.h"
    )

SET(DEPEND_LIBS
  persistency
  ${LIBS}
  )

ADD_LIBRARY(concurrency STATIC ${SRCS})
TARGET_LINK_LIBRARIES(concurrency ${DEPEND_LIBS})
