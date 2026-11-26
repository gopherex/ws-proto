vcpkg_from_git(
    OUT_SOURCE_PATH source
    URL https://github.com/gopherex/ws-proto.git
    REF 5f00e005eb24df6a54de4e0b887978b39daf388d
)

vcpkg_check_features(OUT_FEATURE_OPTIONS feature_options FEATURES asio WS_PROTO_WITH_ASIO)
vcpkg_cmake_configure(SOURCE_PATH "${source}/packages/ws-proto-cpp" OPTIONS ${feature_options})
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/ws-proto)
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_copy_pdbs()
vcpkg_install_copyright(FILE_LIST "${source}/LICENSE")
