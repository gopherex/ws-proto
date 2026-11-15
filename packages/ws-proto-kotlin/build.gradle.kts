import org.jetbrains.kotlin.gradle.tasks.KotlinCompilationTask
import org.jetbrains.kotlin.gradle.targets.native.tasks.KotlinNativeSimulatorTest

plugins {
    kotlin("multiplatform") version "2.2.21"
    id("com.squareup.wire") version "5.4.0"
    `maven-publish`
}

group = "io.gopherex"
val workspaceVersion = Regex("\"version\"\\s*:\\s*\"([^\"]+)\"")
    .find(file("../ws-proto-transport/package.json").readText())!!.groupValues[1]
version = providers.gradleProperty("releaseVersion").getOrElse("$workspaceVersion-SNAPSHOT")

kotlin {
    jvm {
        compilerOptions { jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17 }
        testRuns["test"].executionTask.configure {
            useJUnitPlatform()
            inputs.property("wsProtoTestUrl", providers.environmentVariable("WS_PROTO_TEST_URL").getOrElse(""))
        }
    }
    iosArm64()
    iosSimulatorArm64()
    iosX64()
    sourceSets {
        commonMain.dependencies {
            api("com.squareup.wire:wire-runtime:5.4.0")
            api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
            api("io.ktor:ktor-client-core:3.3.3")
            implementation("io.ktor:ktor-client-websockets:3.3.3")
            implementation("org.jetbrains.kotlinx:atomicfu:0.29.0")
        }
        commonTest {
            kotlin.srcDir(layout.buildDirectory.dir("generated/wire-test"))
            kotlin.srcDir(layout.buildDirectory.dir("generated/ws-test"))
            dependencies {
                implementation(kotlin("test"))
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
            }
        }
        jvmMain.dependencies { implementation("io.ktor:ktor-client-okhttp:3.3.3") }
        jvmTest.dependencies {
            implementation(kotlin("test-junit5"))
            runtimeOnly("org.junit.jupiter:junit-jupiter-engine:5.13.4")
            runtimeOnly("org.junit.platform:junit-platform-launcher")
        }
        iosMain.dependencies { implementation("io.ktor:ktor-client-darwin:3.3.3") }
    }
}
wire {
    sourcePath { srcDir("../../transport") }
    kotlin { rpcRole = "none" }
}

val wireCompiler by configurations.creating
val protocBinary by configurations.creating
val protoSchemas by configurations.creating { isTransitive = false }
val protocPlatform = if (System.getProperty("os.name").startsWith("Mac")) "osx-aarch_64" else "linux-x86_64"
dependencies {
    wireCompiler("com.squareup.wire:wire-compiler:5.4.0")
    protocBinary("com.google.protobuf:protoc:4.36.2:$protocPlatform@exe")
    protoSchemas("com.google.protobuf:protobuf-java:4.36.2")
}
val extractProtoSchemas by tasks.registering(Sync::class) {
    from(provider { zipTree(protoSchemas.singleFile) }) { include("google/protobuf/*.proto") }
    into(layout.buildDirectory.dir("proto-includes"))
}
val generateTestMessages by tasks.registering(JavaExec::class) {
    classpath = wireCompiler
    mainClass = "com.squareup.wire.WireCompiler"
    args("--proto_path=${file("../../example/proto")}", "--proto_path=${file("../../mobilegen/testdata/naming")}",
        "--proto_path=${file("../../mobilegen/testdata/wire")}",
        "--kotlin_out=${layout.buildDirectory.dir("generated/wire-test").get().asFile}",
        "--rpc_role=none", "echo/v1/echo.proto", "messages.proto", "service.proto", "presence.proto")
    inputs.dir("../../example/proto")
    inputs.dir("../../mobilegen/testdata/naming")
    inputs.dir("../../mobilegen/testdata/wire")
    outputs.dir(layout.buildDirectory.dir("generated/wire-test"))
}
val wsGenerator = layout.buildDirectory.file("bin/protoc-gen-ws-kotlin")
val buildWsGenerator by tasks.registering(Exec::class) {
    workingDir = file("../..")
    commandLine("go", "build", "-o", wsGenerator.get().asFile.absolutePath, "./cmd/protoc-gen-ws-kotlin")
    inputs.dir("../../mobilegen")
    inputs.dir("../../cmd/protoc-gen-ws-kotlin")
    inputs.files("../../go.mod", "../../go.sum")
    outputs.file(wsGenerator)
}
val generateTestClients by tasks.registering(Exec::class) {
    dependsOn(buildWsGenerator, extractProtoSchemas)
    val output = layout.buildDirectory.dir("generated/ws-test")
    inputs.dir("../../example/proto")
    inputs.dir("../../mobilegen/testdata/naming")
    inputs.dir("../../mobilegen/testdata/wire")
    inputs.file(wsGenerator)
    outputs.dir(output)
    doFirst {
        output.get().asFile.mkdirs()
        val protoc = protocBinary.singleFile
        protoc.setExecutable(true)
        commandLine(protoc.absolutePath,
            "--proto_path=${file("../../example/proto")}", "--proto_path=${file("../../mobilegen/testdata/naming")}",
            "--proto_path=${file("../../mobilegen/testdata/wire")}", "--proto_path=${layout.buildDirectory.dir("proto-includes").get().asFile}",
            "--plugin=protoc-gen-ws-kotlin=${wsGenerator.get().asFile}",
            "--ws-kotlin_out=${output.get().asFile}", "echo/v1/echo.proto", "messages.proto", "service.proto", "well_known.proto", "presence.proto")
    }
}
tasks.withType<KotlinCompilationTask<*>>().configureEach {
    if (name.contains("Test")) dependsOn(generateTestMessages, generateTestClients)
}
tasks.withType<KotlinNativeSimulatorTest>().configureEach {
    providers.environmentVariable("WS_PROTO_IOS_DEVICE").orNull?.let { deviceId = it }
    environment("SIMCTL_CHILD_WS_PROTO_TEST_URL", providers.environmentVariable("WS_PROTO_TEST_URL").getOrElse(""))
    inputs.property("wsProtoTestUrl", providers.environmentVariable("WS_PROTO_TEST_URL").getOrElse(""))
}

publishing {
    publications.withType<MavenPublication>().configureEach {
        pom {
            name = "ws-proto Kotlin Multiplatform"
            description = "Multiplexed protobuf RPC over WebSocket for Kotlin Multiplatform"
            url = "https://github.com/gopherex/ws-proto"
            licenses { license { name = "MIT"; url = "https://opensource.org/licenses/MIT" } }
            scm { url = "https://github.com/gopherex/ws-proto"; connection = "scm:git:https://github.com/gopherex/ws-proto.git" }
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/gopherex/ws-proto")
            credentials {
                username = providers.environmentVariable("GITHUB_ACTOR").orNull
                password = providers.environmentVariable("GITHUB_TOKEN").orNull
            }
        }
    }
}
