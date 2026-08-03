plugins {
    kotlin("jvm") version "2.0.21"
    kotlin("plugin.compose") version "2.0.21"
    kotlin("plugin.serialization") version "2.0.21"
    id("org.jetbrains.compose") version "1.7.3"
}

group = "io.mex"
version = "3.4.0-alpha"

repositories {
    mavenCentral()
    google()
    maven("https://maven.pkg.jetbrains.space/public/p/compose/dev")
}

kotlin {
    jvmToolchain(21)
}

dependencies {
    // Compose Desktop
    implementation(compose.desktop.currentOs)
    implementation(compose.material3)
    implementation(compose.materialIconsExtended)
    implementation(compose.components.resources)

    // Coroutines + serialization
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-swing:1.9.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.7.3")

    // Mongo driver (sync API + bson)
    implementation("org.mongodb:mongodb-driver-sync:5.2.1")
    implementation("org.mongodb:bson:5.2.1")

    // SQLite
    implementation("org.xerial:sqlite-jdbc:3.46.1.0")

    // ULID
    implementation("com.github.f4b6a3:ulid-creator:5.2.3")

    // Logging
    implementation("org.slf4j:slf4j-simple:2.0.16")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

compose.desktop {
    application {
        mainClass = "io.mex.MainKt"
        nativeDistributions {
            targetFormats(
                org.jetbrains.compose.desktop.application.dsl.TargetFormat.Dmg,
                org.jetbrains.compose.desktop.application.dsl.TargetFormat.Msi,
                org.jetbrains.compose.desktop.application.dsl.TargetFormat.Deb,
            )
            // jlink strips the runtime to the modules listed here; these four are
            // needed at runtime: java.sql (sqlite-jdbc), java.naming (mongodb+srv
            // DNS lookups), jdk.crypto.ec (TLS handshakes), jdk.unsupported (Unsafe).
            modules("java.sql", "java.naming", "jdk.crypto.ec", "jdk.unsupported")
            packageName = "Mongo Explorer v3"
            // Tracks the milestone line (Compose Desktop requires MAJOR >= 1 anyway).
            packageVersion = "3.4.0"
            description = "Mongo Explorer v3 — Kotlin + Compose Desktop rewrite"
            vendor = "io.mex"
            macOS {
                bundleID = "io.mex.explorer.v3"
                iconFile.set(project.file("icon/mex-icon.icns"))
            }
            windows { iconFile.set(project.file("icon/mex-icon.ico")) }
            linux { iconFile.set(project.file("icon/mex-icon.png")) }
        }
    }
}
