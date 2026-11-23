cmake_minimum_required(VERSION 3.22)
project(browser_consumer LANGUAGES CXX)
find_package(ws-proto CONFIG REQUIRED)
add_executable(echo main.cpp "${ECHO_GEN}/proto/echo/v1/echo.pb.cc")
target_include_directories(echo PRIVATE "${ECHO_GEN}")
target_link_libraries(echo PRIVATE ws-proto::client)
target_link_options(echo PRIVATE --no-entry -sALLOW_MEMORY_GROWTH=1
  "-sEXPORTED_FUNCTIONS=_start,_verified,_opened,_message,_closed,_expire,_malloc,_free"
  "-sEXPORTED_RUNTIME_METHODS=HEAPU8,UTF8ToString")
configure_file(index.html index.html COPYONLY)
