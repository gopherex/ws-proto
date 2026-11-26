vcpkg_from_git(
    OUT_SOURCE_PATH source
    URL https://github.com/gopherex/ws-proto.git
    REF 03c49d33a5076faf20d5df2eec3a206a727bf5fa
)

set(VCPKG_BUILD_TYPE release)
vcpkg_cmake_configure(
    SOURCE_PATH "${source}/packages/ws-proto-cpp"
    OPTIONS -DWS_PROTO_BUILD_RUNTIME=OFF -DWS_PROTO_BUILD_TOOLS=ON
)
vcpkg_cmake_install()
vcpkg_copy_tools(TOOL_NAMES protoc-gen-ws-cpp protoc-gen-cpp AUTO_CLEAN)
set(VCPKG_POLICY_EMPTY_INCLUDE_FOLDER enabled)
vcpkg_install_copyright(FILE_LIST "${source}/LICENSE")
