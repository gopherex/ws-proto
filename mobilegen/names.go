package mobilegen

import (
	"path"
	"strings"
	"unicode"

	"google.golang.org/protobuf/reflect/protoreflect"
)

func validIdentifier(name string) bool {
	if name == "" {
		return false
	}
	for i, c := range name {
		if c != '_' && !(c >= 'A' && c <= 'Z') && !(c >= 'a' && c <= 'z') && !(i > 0 && c >= '0' && c <= '9') {
			return false
		}
	}
	return true
}

func validPackage(name string) bool {
	if name == "" {
		return true
	}
	for _, part := range strings.Split(name, ".") {
		if !validIdentifier(part) {
			return false
		}
	}
	return true
}

func validJavaIdentifier(name string) bool {
	const reserved = " abstract assert boolean break byte case catch char class const continue default do double else enum extends final finally float for goto if implements import instanceof int interface long native new package private protected public return short static strictfp super switch synchronized this throw throws transient try void volatile while true false null _ var yield record sealed permits "
	return validIdentifier(name) && !strings.Contains(reserved, " "+name+" ")
}

func validJavaMessage(message protoreflect.MessageDescriptor) bool {
	file := message.ParentFile()
	if !validJavaIdentifier(javaOuter(file)) {
		return false
	}
	if pkg := javaPackage(file); pkg != "" {
		for _, part := range strings.Split(pkg, ".") {
			if !validJavaIdentifier(part) {
				return false
			}
		}
	}
	for parent := protoreflect.Descriptor(message); parent != nil; parent = parent.Parent() {
		switch parent := parent.(type) {
		case protoreflect.MessageDescriptor:
			if !validJavaIdentifier(string(parent.Name())) {
				return false
			}
		}
	}
	return true
}

func methodName(name string) string {
	if name == "" {
		return name
	}
	name = strings.ToLower(name[:1]) + name[1:]
	// Constructors cannot be declared as ordinary Swift instance methods.
	if name == "init" || name == "deinit" || name == "self" {
		return name + "RPC"
	}
	return name
}

func javaPackage(file protoreflect.FileDescriptor) string {
	options := fileOptions(file)
	if options.JavaPackage != nil {
		return options.GetJavaPackage()
	}
	return string(file.Package())
}

func javaCamel(name string) string {
	var result strings.Builder
	upper := true
	for _, c := range name {
		switch {
		case c >= 'a' && c <= 'z':
			if upper {
				c = unicode.ToUpper(c)
			}
			result.WriteRune(c)
			upper = false
		case c >= 'A' && c <= 'Z':
			result.WriteRune(c)
			upper = false
		case c >= '0' && c <= '9':
			result.WriteRune(c)
			upper = true
		default:
			upper = true
		}
	}
	return result.String()
}

func javaOuter(file protoreflect.FileDescriptor) string {
	options := fileOptions(file)
	if options.JavaOuterClassname != nil {
		return options.GetJavaOuterClassname()
	}
	name := javaCamel(strings.TrimSuffix(path.Base(file.Path()), path.Ext(file.Path())))
	if javaNameCollision(file.Messages(), file.Enums(), name) {
		return name + "OuterClass"
	}
	for i := 0; i < file.Services().Len(); i++ {
		if string(file.Services().Get(i).Name()) == name {
			return name + "OuterClass"
		}
	}
	return name
}

func javaNameCollision(messages protoreflect.MessageDescriptors, enums protoreflect.EnumDescriptors, name string) bool {
	for i := 0; i < enums.Len(); i++ {
		if string(enums.Get(i).Name()) == name {
			return true
		}
	}
	for i := 0; i < messages.Len(); i++ {
		message := messages.Get(i)
		if string(message.Name()) == name || javaNameCollision(message.Messages(), message.Enums(), name) {
			return true
		}
	}
	return false
}

func kotlinName(message protoreflect.MessageDescriptor) string {
	name := string(message.Name())
	for parent := message.Parent(); parent != nil; parent = parent.Parent() {
		switch parent := parent.(type) {
		case protoreflect.MessageDescriptor:
			name = string(parent.Name()) + "." + name
		}
	}
	file := message.ParentFile()
	if !fileOptions(file).GetJavaMultipleFiles() {
		name = javaOuter(file) + "." + name
	}
	if pkg := javaPackage(file); pkg != "" {
		name = pkg + "." + name
	}
	return kotlinQualified(name)
}

func kotlinQualified(name string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		parts[i] = "`" + part + "`"
	}
	return strings.Join(parts, ".")
}

func swiftPrefix(file protoreflect.FileDescriptor) string {
	options := fileOptions(file)
	if options.SwiftPrefix != nil {
		return options.GetSwiftPrefix()
	}
	if file.Package() == "" {
		return ""
	}
	var result strings.Builder
	upper := true
	for _, c := range file.Package() {
		switch c {
		case '_':
			upper = true
		case '.':
			result.WriteByte('_')
			upper = true
		default:
			if result.Len() == 0 && c >= '0' && c <= '9' {
				result.WriteByte('_')
			}
			if upper {
				c = unicode.ToUpper(c)
				upper = false
			}
			result.WriteRune(c)
		}
	}
	return result.String() + "_"
}

// SwiftProtobuf preserves message spelling. Only its package prefix is
// camel-cased; reserved message names receive the Message disambiguator.
func swiftMessageName(name string) string {
	const reserved = " SwiftProtobuf Extensions protoMessageName decodeMessage traverse isInitialized unknownFields debugDescription description dynamicType hashValue Type Protocol Swift Equatable Hashable Sendable associatedtype class deinit enum extension fileprivate func import init inout internal let open operator private protocol public static struct subscript typealias var break case continue default defer do else fallthrough for guard if in repeat return switch where while as Any catch false is nil rethrows super self Self throw throws true try Bool Data Double Float Int Int32 Int64 String UInt UInt32 UInt64 __COLUMN__ __FILE__ __FUNCTION__ __LINE__ "
	if strings.Contains(reserved, " "+name+" ") || (name != "" && strings.Trim(name, "_") == "") {
		return name + "Message"
	}
	if strings.HasSuffix(name, "Message") {
		return swiftMessageName(strings.TrimSuffix(name, "Message")) + "Message"
	}
	return name
}

func swiftName(message protoreflect.MessageDescriptor, current protoreflect.FileDescriptor, options settings) string {
	parts := []string{string(message.Name())}
	for parent := message.Parent(); parent != nil; parent = parent.Parent() {
		switch parent := parent.(type) {
		case protoreflect.MessageDescriptor:
			parts = append([]string{string(parent.Name())}, parts...)
		}
	}
	file := message.ParentFile()
	parts[0] = swiftPrefix(file) + parts[0]
	for i, part := range parts {
		parts[i] = swiftMessageName(part)
	}
	name := strings.Join(parts, ".")
	module := swiftModule(file.Path(), options)
	if module != "" && module != swiftModule(current.Path(), options) {
		name = module + "." + name
	}
	return name
}

func swiftModule(file string, options settings) string {
	if module, exists := options.modules[file]; exists {
		return module
	}
	// Only well-known types ship with SwiftProtobuf; arbitrary files under
	// google/protobuf may be generated by the application itself.
	switch file {
	case "google/protobuf/any.proto", "google/protobuf/api.proto", "google/protobuf/duration.proto", "google/protobuf/empty.proto", "google/protobuf/field_mask.proto", "google/protobuf/source_context.proto", "google/protobuf/struct.proto", "google/protobuf/timestamp.proto", "google/protobuf/type.proto", "google/protobuf/wrappers.proto":
		return "SwiftProtobuf"
	default:
		return ""
	}
}
