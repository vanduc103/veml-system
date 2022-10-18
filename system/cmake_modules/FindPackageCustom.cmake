# Custom find package function for clean file
function(FIND_PACKAGE_CUSTOM _package_ _root_path_ _header_file_ _libraries_ _is_include_ _is_adding_lib_)
    # Clear cached variable
    unset(INCLUDE_DIR CACHE)
    set(LIBRARIES "")

    if(${_is_include_})
        unset(INCLUDE_PATH CACHE)
        foreach(RPATH ${_root_path_})
            set(INCLUDE_PATH ${INCLUDE_PATH} ${RPATH}/include)
        endforeach(RPATH)

        find_path(INCLUDE_DIR ${_header_file_}
            PATHS ${INCLUDE_PATH} NO_DEFAULT_PATH
        )

        if(INCLUDE_DIR)
            include_directories(SYSTEM ${INCLUDE_DIR})
            message(STATUS "${_package_} include dir: ${INCLUDE_DIR}")
        else(INCLUDE_DIR)
            message(FATAL_ERROR "Could not find the ${_package_} include dir")
        endif(INCLUDE_DIR)
    endif(${_is_include_})

    if(${_is_adding_lib_})
        unset(LIB_PATH CACHE)
        foreach(RPATH ${_root_path_})
            set(LIB_PATH ${LIB_PATH} ${RPATH}/lib)
            set(LIB_PATH ${LIB_PATH} ${RPATH}/lib64)
        endforeach(RPATH)

        foreach(LIB_NAME ${_libraries_})
            unset(LIBRARY CACHE)
            find_library(LIBRARY NAMES ${LIB_NAME}
                PATHS ${LIB_PATH} NO_DEFAULT_PATH
            )

            if(LIBRARY)
                set(LIBRARIES ${LIBRARIES} ${LIBRARY})
	        else(LIBRARY)
	            message(FATAL_ERROR "Could not find the ${LIB_NAME} libraries")
            endif(LIBRARY)
        endforeach(LIB_NAME)

        if(LIBRARIES)
            set(LIBS ${LIBS} ${LIBRARIES} PARENT_SCOPE)
            message(STATUS "${_package_} libraries: ${LIBRARIES}")
        else(LIBRARIES)
            message(FATAL_ERROR "Could not find the ${_package_} libraries")
        endif(LIBRARIES)
    endif(${_is_adding_lib_})
endfunction(FIND_PACKAGE_CUSTOM)

function(FIND_PACKAGE_CUSTOM_TEST _package_ _root_path_ _header_file_ _libraries_ _is_include_ _is_adding_lib_)
    # Clear cached variable
    unset(INCLUDE_DIR CACHE)
    set(LIBRARIES "")

    if(${_is_include_})
        unset(INCLUDE_PATH CACHE)
        foreach(RPATH ${_root_path_})
            set(INCLUDE_PATH ${INCLUDE_PATH} ${RPATH}/include)
        endforeach(RPATH)

        find_path(INCLUDE_DIR ${_header_file_}
            PATHS ${INCLUDE_PATH} NO_DEFAULT_PATH
        )

        if(INCLUDE_DIR)
            include_directories(SYSTEM ${INCLUDE_DIR})
            message(STATUS "${_package_} include dir: ${INCLUDE_DIR}")
        else(INCLUDE_DIR)
            message(FATAL_ERROR "Could not find the ${_package_} include dir")
        endif(INCLUDE_DIR)
    endif(${_is_include_})

    if(${_is_adding_lib_})
        unset(LIB_PATH CACHE)
        foreach(RPATH ${_root_path_})
            set(LIB_PATH ${LIB_PATH} ${RPATH}/lib)
            set(LIB_PATH ${LIB_PATH} ${RPATH}/lib64)
        endforeach(RPATH)

        foreach(LIB_NAME ${_libraries_})
            unset(LIBRARY CACHE)
            find_library(LIBRARY NAMES ${LIB_NAME}
                PATHS ${LIB_PATH} NO_DEFAULT_PATH
            )

            if(LIBRARY)
                set(LIBRARIES ${LIBRARIES} ${LIBRARY})
	        else(LIBRARY)
	            message(FATAL_ERROR "Could not find the ${LIB_NAME} libraries")
            endif(LIBRARY)
        endforeach(LIB_NAME)

        if(LIBRARIES)
            set(TEST_ONLY_LIBS ${TEST_ONLY_LIBS} ${LIBRARIES} PARENT_SCOPE)
            message(STATUS "${_package_} libraries: ${LIBRARIES}")
        else(LIBRARIES)
            message(FATAL_ERROR "Could not find the ${_package_} libraries")
        endif(LIBRARIES)
    endif(${_is_adding_lib_})
endfunction(FIND_PACKAGE_CUSTOM_TEST)
