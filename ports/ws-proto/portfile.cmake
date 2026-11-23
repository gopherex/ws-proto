vcpkg_from_git(
    OUT_SOURCE_PATH source
    URL https://github.com/gopherex/ws-proto.git
    REF 35c044b08a0bab50e94b030bdd17ccd1c569e24d
)

vcpkg_cmake_configure(SOURCE_PATH "${source}/packages/ws-proto-cpp")
vcpkg_cmake_install()
vcpkg_cmake_config_fixup(CONFIG_PATH lib/cmake/ws-proto)
file(REMOVE_RECURSE "${CURRENT_PACKAGES_DIR}/debug/include")
vcpkg_copy_pdbs()
vcpkg_install_copyright(FILE_LIST "${source}/LICENSE")
