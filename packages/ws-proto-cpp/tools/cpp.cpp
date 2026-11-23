#include <google/protobuf/compiler/cpp/generator.h>
#include <google/protobuf/compiler/plugin.h>

// Expose the installed protobuf C++ generator through the stdin/stdout plugin
// protocol so EasyP and the target library can use the same protobuf release.
int main(int argc, char** argv) {
    google::protobuf::compiler::cpp::CppGenerator generator;
    return google::protobuf::compiler::PluginMain(argc, argv, &generator);
}
