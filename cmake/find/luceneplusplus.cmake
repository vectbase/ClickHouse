option(ENABLE_LUCENE "Enable LUCENE" ${ENABLE_LIBRARIES})

if (NOT ENABLE_LUCENE)
    if (USE_INTERNAL_LUCENE_LIBRARY)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't use internal lucene library with ENABLE_LUCENE=OFF")
    endif()
    return()
endif()

option(USE_INTERNAL_LUCENE_LIBRARY "Set to FALSE to use system LUCENE library instead of bundled" ${NOT_UNBUNDLED})

if (NOT EXISTS "${ClickHouse_SOURCE_DIR}/contrib/LucenePlusPlus/CMakeLists.txt")
    if (USE_INTERNAL_LUCENE_LIBRARY)
        message (WARNING "submodule contrib is missing. to fix try run: \n git submodule update --init --recursive")
        message(${RECONFIGURE_MESSAGE_LEVEL} "cannot find internal lucene")
    endif()
    set (MISSING_INTERNAL_LUCENE 1)
endif ()

if (NOT USE_INTERNAL_LUCENE_LIBRARY)
    find_library (LUCENE_LIBRARY lucene++)
    find_path (LUCENE_INCLUDE_DIR NAMES lucene++/LuceneHeaders.h PATHS ${LUCENE_INCLUDE_PATHS})
    if (NOT LUCENE_LIBRARY OR NOT LUCENE_INCLUDE_DIR)
        message (${RECONFIGURE_MESSAGE_LEVEL} "Can't find system lucene library")
    endif()

    if (NOT ZLIB_LIBRARY)
        include(cmake/find/zlib.cmake)
    endif()

    if(ZLIB_LIBRARY)
        list (APPEND LUCENE_LIBRARY ${ZLIB_LIBRARY})
    else()
        message (${RECONFIGURE_MESSAGE_LEVEL}
                 "Can't find system lucene: zlib=${ZLIB_LIBRARY} ;")
    endif()
endif ()

if(LUCENE_LIBRARY AND LUCENE_INCLUDE_DIR)
    set(USE_LUCENE 1)
elseif (NOT MISSING_INTERNAL_LUCENE)
    set (USE_INTERNAL_LUCENE_LIBRARY 1)

    set (LUCENE_INCLUDE_DIR "${ClickHouse_SOURCE_DIR}/contrib/LucenePlusPlus/include")
    set (LUCENE_LIBRARY "lucene++")
    set (USE_LUCENE 1)
endif ()

message (STATUS "Using LUCENE=${USE_LUCENE}: ${LUCENE_INCLUDE_DIR} : ${LUCENE_LIBRARY}")
