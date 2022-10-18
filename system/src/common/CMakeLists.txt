# Set files to create library
# If you add new source file(s), add file's name to following lines

SET(HDRS
    "def_const.h"
    )

# Set sub-directories to create library
ADD_SUBDIRECTORY(types)

# Define installer
INSTALL(FILES ${HDRS} DESTINATION include/common)
