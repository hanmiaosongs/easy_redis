cmake_minimum_required(VERSION 3.15)
project(redis_server)

# set the path to head files
include_directories(${PROJECT_SOURCE_DIR}/include)

set(home /home/error/viscode/redis_serve)
set(EXECUTABLE_OUTPUT_PATH ${home})
set(CMAKE_CXX_STANDARD 14)

# aux_source_directory(${PROJECT_SOURCE_DIR}/src source_list)          # PROJECT_SOURCE_DIR => the loc of CMakeLists.txt
# file(GLOB source_list ${home}/src/*.cpp)
# add_executable(app ${source_list})


# use of static lib
#link_directories(${PROJECT_SOURCE_DIR}/lib)                    # path of th static lib
#link_libraries(redis)
#add_executable(app ${home}/src/main.cpp)


# use of SHARED lib
#link_directories(${PROJECT_SOURCE_DIR}/lib)
#add_executable(app ${home}/src/main.cpp)
#target_link_libraries(app libredis.so)

# build library
#set(home /home/error/viscode/redis_serve/src)
#set(LIBRARY_OUTPUT_PATH ${PROJECT_SOURCE_DIR}/lib)
#set(source_list ${home}/AVL.cpp;${home}/hashtable.cpp;${home}/zset.cpp)
#add_library(reids STATIC ${source_list})                       #SHARED STATIC

