# Set files to create library
# If you add new source file(s), add file's name to following lines
SET(SRCS
    buffer
    result_checker
    )

ADD_LIBRARY(util STATIC ${SRCS})
TARGET_LINK_LIBRARIES(util ${DEPEND_LIBS})

ADD_EXECUTABLE(graph_gen graph_gen)
TARGET_LINK_LIBRARIES(graph_gen concurrency)
