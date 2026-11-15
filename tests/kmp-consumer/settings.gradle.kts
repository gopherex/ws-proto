pluginManagement {
    repositories { mavenCentral(); gradlePluginPortal() }
}

val publishedVersion = providers.gradleProperty("wsProtoVersion").orNull
dependencyResolutionManagement {
    repositories {
        if (publishedVersion != null) {
            require(!publishedVersion.endsWith("-SNAPSHOT")) { "Published consumer requires a release version" }
            exclusiveContent {
                forRepository {
                    maven {
                        name = "wsProtoGitHubPackages"
                        url = uri("https://maven.pkg.github.com/gopherex/ws-proto")
                        credentials {
                            username = providers.environmentVariable("GITHUB_ACTOR").orNull
                            password = providers.environmentVariable("GITHUB_TOKEN").orNull
                        }
                    }
                }
                filter { includeGroup("io.gopherex") }
            }
        }
        mavenCentral()
    }
}

rootProject.name = "ws-proto-kmp-consumer"
if (publishedVersion == null) {
    includeBuild("../../packages/ws-proto-kotlin")
}
