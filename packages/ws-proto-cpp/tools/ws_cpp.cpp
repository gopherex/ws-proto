#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/cpp/names.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/printer.h>

#include <memory>
#include <set>
#include <sstream>
#include <vector>

namespace protobuf = google::protobuf;
namespace compiler = protobuf::compiler;
namespace cpp = compiler::cpp;

namespace {
// Protobuf flattens nested messages and enums into their package's C++ namespace.
void collect_enum(const protobuf::EnumDescriptor* type, std::set<std::string>& names) {
    auto qualified = cpp::QualifiedClassName(type);
    names.insert(qualified);
    auto prefix = type->containing_type() ? qualified + "_" : cpp::Namespace(type) + "::";
    for (int i = 0; i < type->value_count(); ++i)
        names.insert(prefix + cpp::EnumValueName(type->value(i)));
}

void collect_message(const protobuf::Descriptor* type, std::set<std::string>& names) {
    names.insert(cpp::QualifiedClassName(type));
    for (int i = 0; i < type->enum_type_count(); ++i) collect_enum(type->enum_type(i), names);
    for (int i = 0; i < type->nested_type_count(); ++i) collect_message(type->nested_type(i), names);
}

void collect_file(const protobuf::FileDescriptor* file, std::set<const protobuf::FileDescriptor*>& seen,
                  std::set<std::string>& names) {
    if (!seen.insert(file).second) return;
    auto ns = cpp::Namespace(file);
    for (auto scope = ns; !scope.empty(); scope.erase(scope.rfind("::"))) names.insert(scope);
    for (int i = 0; i < file->message_type_count(); ++i) collect_message(file->message_type(i), names);
    for (int i = 0; i < file->enum_type_count(); ++i) collect_enum(file->enum_type(i), names);
    for (int i = 0; i < file->extension_count(); ++i) names.insert(cpp::QualifiedExtensionName(file->extension(i)));
    for (int i = 0; i < file->service_count(); ++i) names.insert(ns + "::" + std::string(file->service(i)->name()));
    for (int i = 0; i < file->dependency_count(); ++i) collect_file(file->dependency(i), seen, names);
}
} // namespace

class ClientGenerator final : public compiler::CodeGenerator {
public:
    uint64_t GetSupportedFeatures() const override { return FEATURE_PROTO3_OPTIONAL; }

    bool Generate(const protobuf::FileDescriptor* file, const std::string& parameter,
                  compiler::GeneratorContext* context, std::string* error) const override {
        return GenerateAll({file}, parameter, context, error);
    }

    bool GenerateAll(const std::vector<const protobuf::FileDescriptor*>& files, const std::string& parameter,
                     compiler::GeneratorContext* context, std::string* error) const override {
        if (!parameter.empty()) {
            *error = "ws-cpp does not accept generator options";
            return false;
        }
        std::set<const protobuf::FileDescriptor*> seen;
        std::set<std::string> names;
        for (auto* file : files) collect_file(file, seen, names);
        for (auto* file : files)
            if (file->service_count() && !generate(file, names, context)) return false;
        return true;
    }

private:
    static bool generate(const protobuf::FileDescriptor* file, const std::set<std::string>& names,
                         compiler::GeneratorContext* context) {
        std::set<std::string> includes;
        includes.insert(cpp::StripProto(file->name()) + ".pb.h");
        std::ostringstream body;
        auto ns = cpp::Namespace(file);
        if (ns.starts_with("::")) ns.erase(0, 2);
        if (!ns.empty()) body << "namespace " << ns << " {\n";
        for (int i = 0; i < file->service_count(); ++i) {
            const auto* service = file->service(i);
            const auto base = std::string(service->name()) + "Client";
            auto name = base;
            const auto prefix = cpp::Namespace(file) + "::";
            auto conflicts = [&](const std::string& candidate) {
                if (names.contains(prefix + candidate) || names.contains(prefix + candidate + "T")) return true;
                for (std::string_view member : {"call_", "method_"})
                    if (candidate.starts_with(member) && service->FindMethodByName(candidate.substr(member.size()) + "T")) return true;
                return false;
            };
            for (int suffix = 2; conflicts(name); ++suffix) name = base + std::to_string(suffix);
            body << "\ntemplate<class Channel>\nclass " << name << "T {\npublic:\n"
                 << "    explicit " << name << "T(Channel& channel) : channel_(channel) {}\n";
            for (int j = 0; j < service->method_count(); ++j) {
                const auto* method = service->method(j);
                auto request = cpp::QualifiedClassName(method->input_type());
                auto response = cpp::QualifiedClassName(method->output_type());
                for (auto* type : {method->input_type(), method->output_type()})
                    includes.insert(cpp::StripProto(type->file()->name()) + ".pb.h");
                const auto member = "call_" + std::string(method->name());
                auto descriptor = "method_" + std::string(method->name());
                body << "\n    static constexpr auto " << descriptor << "() {\n"
                     << "        using Rpc = ::ws_proto::Rpc<" << request << ", " << response << ", "
                     << std::boolalpha << method->client_streaming() << ", " << method->server_streaming() << ">;\n"
                     << "        return ::ws_proto::Method<Rpc>{\"/" << service->full_name() << "/" << method->name()
                     << "\"};\n    }\n"
                     << "    auto " << member << "(";
                if (!method->client_streaming()) body << "const " << request << "& request, ";
                body << "::ws_proto::Options options = {}) const {\n"
                     << "        return channel_.invoke(" << descriptor << "(), ::std::move(options)";
                if (!method->client_streaming()) body << ", request";
                body << ");\n    }\n";
            }
            body << "private:\n    Channel& channel_;\n};\n"
                 << "using " << name << " = " << name << "T<::ws_proto::Session>;\n";
        }
        if (!ns.empty()) body << "} // namespace " << ns << "\n";
        std::ostringstream header;
        header << "// Generated by protoc-gen-ws-cpp. DO NOT EDIT.\n#pragma once\n\n#include <ws_proto/client.hpp>\n";
        for (const auto& include : includes) header << "#include \"" << include << "\"\n";
        header << "\n" << body.str();
        std::unique_ptr<protobuf::io::ZeroCopyOutputStream> out(context->Open(cpp::StripProto(file->name()) + ".ws.pb.h"));
        protobuf::io::Printer printer(out.get(), '$');
        printer.PrintRaw(header.str());
        return !printer.failed();
    }
};

int main(int argc, char** argv) {
    ClientGenerator generator;
    return compiler::PluginMain(argc, argv, &generator);
}
