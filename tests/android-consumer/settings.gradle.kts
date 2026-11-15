pluginManagement {
    repositories { google(); mavenCentral(); gradlePluginPortal() }
    resolutionStrategy {
        eachPlugin {
            if (requested.id.id == "com.android.application") {
                useModule("com.android.tools.build:gradle:${requested.version}")
            }
        }
    }
}
dependencyResolutionManagement {
    repositories {
        google(); mavenCentral()
        maven {
            url = uri("https://maven.pkg.github.com/gopherex/ws-proto")
            credentials {
                username = providers.environmentVariable("GITHUB_ACTOR").orNull
                password = providers.environmentVariable("GITHUB_TOKEN").orNull
            }
            content { includeGroup("io.gopherex") }
        }
    }
}
rootProject.name = "ws-proto-android-consumer"
if (!providers.gradleProperty("wsProtoVersion").orElse(providers.environmentVariable("WS_PROTO_VERSION")).isPresent) {
    includeBuild("../../packages/ws-proto-kotlin")
}
