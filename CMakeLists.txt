cmake_minimum_required(VERSION 3.10)
project(brc-cpp CXX)

set(CMAKE_CXX_STANDARD 11)

add_executable(brc main.cpp)
target_compile_options(brc PRIVATE -O3)
