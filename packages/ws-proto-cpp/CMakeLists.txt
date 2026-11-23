cmake_minimum_required(VERSION 3.22)
project(ws-proto LANGUAGES CXX)
include(GNUInstallDirs)
include(CMakePackageConfigHelpers)
option(WS_PROTO_BUILD_RUNTIME "Build the client library" ON)
option(WS_PROTO_BUILD_TOOLS "Build native protoc/EasyP plugins" OFF)
option(WS_PROTO_BUILD_TESTS "Build the client protocol tests" OFF)
find_package(Protobuf CONFIG REQUIRED)

if(WS_PROTO_BUILD_TOOLS)
  foreach(tool IN ITEMS cpp ws_cpp)
    string(REPLACE "_" "-" plugin "${tool}")
    add_executable(protoc-gen-${plugin} tools/${tool}.cpp)
    target_compile_features(protoc-gen-${plugin} PRIVATE cxx_std_23)
    target_link_libraries(protoc-gen-${plugin} PRIVATE protobuf::libprotoc protobuf::libprotobuf)
    install(TARGETS protoc-gen-${plugin} RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
  endforeach()
endif()

if(WS_PROTO_BUILD_RUNTIME)
  set(proto_root "${CMAKE_CURRENT_SOURCE_DIR}/../..")
  set(gen "${CMAKE_CURRENT_BINARY_DIR}/gen")
  set(frame "transport/transport.pb")
  # The vcpkg protobuf wrapper resolves this executable for the host triplet.
  if(NOT Protobuf_PROTOC_EXECUTABLE)
    set(Protobuf_PROTOC_EXECUTABLE protobuf::protoc)
  endif()
  add_custom_command(OUTPUT "${gen}/${frame}.cc" "${gen}/${frame}.h"
    COMMAND "${CMAKE_COMMAND}" -E make_directory "${gen}"
    COMMAND "${Protobuf_PROTOC_EXECUTABLE}" "--proto_path=${proto_root}" "--cpp_out=${gen}" transport/transport.proto
    DEPENDS "${proto_root}/transport/transport.proto" ${Protobuf_PROTOC_EXECUTABLE}
    VERBATIM)
  add_library(ws_proto_client src/client.cpp "${gen}/${frame}.cc")
  add_library(ws-proto::client ALIAS ws_proto_client)
  set_target_properties(ws_proto_client PROPERTIES EXPORT_NAME client POSITION_INDEPENDENT_CODE ON
    WINDOWS_EXPORT_ALL_SYMBOLS ON)
  target_compile_features(ws_proto_client PUBLIC cxx_std_23)
  target_include_directories(ws_proto_client PUBLIC "$<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>"
    "$<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>" PRIVATE "${gen}")
  target_link_libraries(ws_proto_client PUBLIC protobuf::libprotobuf)
  install(TARGETS ws_proto_client EXPORT ws-proto-targets)
  install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_INCLUDEDIR})
  set(config_dir "${CMAKE_INSTALL_LIBDIR}/cmake/ws-proto")
  install(EXPORT ws-proto-targets NAMESPACE ws-proto:: DESTINATION "${config_dir}")
  configure_package_config_file(ws-proto-config.cmake.in "${CMAKE_CURRENT_BINARY_DIR}/ws-proto-config.cmake"
    INSTALL_DESTINATION "${config_dir}")
  install(FILES "${CMAKE_CURRENT_BINARY_DIR}/ws-proto-config.cmake" DESTINATION "${config_dir}")
  if(WS_PROTO_BUILD_TESTS)
    enable_testing()
    find_package(GTest CONFIG REQUIRED)
    find_package(benchmark CONFIG REQUIRED)
    add_executable(runtime_test ../../tests/cpp/runtime_test.cpp)
    target_include_directories(runtime_test PRIVATE "${gen}")
    target_link_libraries(runtime_test PRIVATE ws-proto::client GTest::gtest_main)
    add_test(NAME runtime COMMAND runtime_test)
    add_executable(client_benchmark ../../tests/cpp/client_benchmark.cpp)
    target_link_libraries(client_benchmark PRIVATE ws-proto::client benchmark::benchmark)
    if(WS_PROTO_BUILD_TOOLS)
      set(fixtures "${proto_root}/mobilegen/testdata")
      find_path(wkt_root google/protobuf/empty.proto REQUIRED)
      set(schema_sources "${gen}/service.pb.cc" "${gen}/messages.pb.cc" "${gen}/well_known.pb.cc" "${gen}/cpp_names.pb.cc")
      add_custom_command(OUTPUT ${schema_sources} "${gen}/service.ws.pb.h" "${gen}/well_known.ws.pb.h" "${gen}/cpp_names.ws.pb.h"
        COMMAND "${CMAKE_COMMAND}" -E make_directory "${gen}"
        COMMAND "${Protobuf_PROTOC_EXECUTABLE}" "-I${fixtures}/naming" "-I${fixtures}/wire" "-I${proto_root}/tests/cpp" "-I${wkt_root}"
          "--cpp_out=${gen}" "--ws-cpp_out=${gen}" "--plugin=protoc-gen-ws-cpp=$<TARGET_FILE:protoc-gen-ws-cpp>"
          service.proto messages.proto well_known.proto cpp_names.proto
        DEPENDS protoc-gen-ws-cpp "${fixtures}/naming/service.proto" "${fixtures}/naming/messages.proto"
          "${fixtures}/wire/well_known.proto" "${proto_root}/tests/cpp/cpp_names.proto" VERBATIM)
      add_executable(schema_test ../../tests/cpp/schema_test.cpp ${schema_sources})
      target_compile_options(schema_test PRIVATE "$<$<CXX_COMPILER_ID:GNU,Clang,AppleClang>:-Werror=unused-result>")
      target_include_directories(schema_test PRIVATE "${gen}")
      target_link_libraries(schema_test PRIVATE ws-proto::client GTest::gtest_main)
      add_test(NAME schema COMMAND schema_test)
    endif()
  endif()
endif()
