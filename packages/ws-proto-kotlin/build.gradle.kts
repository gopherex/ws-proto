import com.google.protobuf.gradle.*

plugins {
    kotlin("jvm") version "2.2.21"
    `java-library`
    `maven-publish`
    id("com.google.protobuf") version "0.10.1"
}

group = "io.gopherex"
val workspaceVersion = Regex("\"version\"\\s*:\\s*\"([^\"]+)\"")
    .find(file("../ws-proto-transport/package.json").readText())!!.groupValues[1]
version = providers.gradleProperty("releaseVersion").getOrElse("$workspaceVersion-SNAPSHOT")

java {
    sourceCompatibility = JavaVersion.VERSION_17
    targetCompatibility = JavaVersion.VERSION_17
    withSourcesJar()
    withJavadocJar()
}
kotlin { compilerOptions { jvmTarget = org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17 } }

dependencies {
    api("com.google.protobuf:protobuf-javalite:4.36.2")
    api("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
    api("com.squareup.okhttp3:okhttp:4.12.0")
    testImplementation(kotlin("test"))
    testImplementation("org.junit.jupiter:junit-jupiter:5.13.4")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.jetbrains.kotlinx:kotlinx-coroutines-test:1.10.2")
}

sourceSets {
    main { proto { srcDir("../../transport") } }
    test { proto { srcDir("../../example/proto"); srcDir("../../mobilegen/testdata/naming") } }
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
kotlin.sourceSets.test { kotlin.srcDir(layout.buildDirectory.dir("generated/sources/proto/test/wsKotlin")) }
protobuf {
    protoc { artifact = "com.google.protobuf:protoc:4.36.2" }
    plugins { id("wsKotlin") { path = wsGenerator.get().asFile.absolutePath } }
    generateProtoTasks {
        all().configureEach { builtins { named("java") { option("lite") } } }
        ofSourceSet("test").configureEach {
            dependsOn(buildWsGenerator)
            plugins { id("wsKotlin") }
        }
    }
}
tasks.test {
    useJUnitPlatform()
    inputs.property("wsProtoTestUrl", providers.environmentVariable("WS_PROTO_TEST_URL").getOrElse(""))
    systemProperty("junit.jupiter.execution.parallel.enabled", "true")
    systemProperty("junit.jupiter.execution.parallel.mode.default", "concurrent")
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifactId = "ws-proto-kotlin"
            pom {
                name = "ws-proto Kotlin"
                description = "Multiplexed protobuf RPC over WebSocket for Android and JVM"
                url = "https://github.com/gopherex/ws-proto"
                licenses { license { name = "MIT"; url = "https://opensource.org/licenses/MIT" } }
                scm { url = "https://github.com/gopherex/ws-proto"; connection = "scm:git:https://github.com/gopherex/ws-proto.git" }
            }
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
