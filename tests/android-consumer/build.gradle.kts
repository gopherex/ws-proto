plugins {
    id("com.android.application") version "9.2.1"
}

android {
    namespace = "io.gopherex.wsproto.consumer"
    compileSdk = 35
    defaultConfig {
        applicationId = "io.gopherex.wsproto.consumer"
        minSdk = 26
        targetSdk = 35
        versionCode = 1
        versionName = "1.0"
    }
    compileOptions {
        sourceCompatibility = JavaVersion.VERSION_17
        targetCompatibility = JavaVersion.VERSION_17
    }
}

dependencies {
    val wsProtoVersion = providers.gradleProperty("wsProtoVersion")
        .orElse(providers.environmentVariable("WS_PROTO_VERSION")).getOrElse("1.0")
    implementation("io.gopherex:ws-proto-kotlin:$wsProtoVersion")
}
