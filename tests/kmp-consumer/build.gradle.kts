plugins {
    kotlin("multiplatform") version "2.2.21"
    id("com.squareup.wire") version "5.4.0"
    id("co.touchlab.skie") version "0.10.7"
}

val publishedVersion = providers.gradleProperty("wsProtoVersion").orNull
val generatedClients = layout.buildDirectory.dir("generated/ws")
val generateWsClient by tasks.registering(Exec::class) {
    workingDir = file("../..")
    commandLine(
        "go", "run", "./mobilegen/testdata/generate.go", "-language", "kotlin",
        "-out", generatedClients.get().asFile.absolutePath,
    )
    inputs.dir("../../mobilegen")
    inputs.dir("../../example/proto")
    inputs.files("../../go.mod", "../../go.sum")
    outputs.dir(generatedClients)
}

kotlin {
    jvm {
        compilerOptions { jvmTarget.set(org.jetbrains.kotlin.gradle.dsl.JvmTarget.JVM_17) }
    }
    listOf(iosArm64(), iosSimulatorArm64(), iosX64()).forEach { target ->
        target.binaries.framework {
            baseName = "KmpFixture"
            isStatic = true
        }
    }
    sourceSets {
        commonMain {
            kotlin.srcDir(generatedClients)
            dependencies {
                implementation("io.gopherex:ws-proto-kotlin:${publishedVersion ?: "1.0"}")
                implementation("com.squareup.wire:wire-runtime:5.4.0")
                implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.10.2")
            }
        }
    }
}

wire {
    sourcePath { srcDir("../../example/proto") }
    kotlin { rpcRole = "none" }
}

skie {
    analytics { enabled.set(false) }
}

tasks.matching { it.name.startsWith("compileKotlin") }.configureEach {
    dependsOn(generateWsClient)
}
