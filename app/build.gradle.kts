plugins {
    application
    id("org.openjfx.javafxplugin") version "0.1.0"
    id("org.beryx.runtime") version "1.13.1"
}

group = "com.kubrik.mex"
version = "1.0.0"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(21))
    }
}

repositories {
    mavenCentral()
}

javafx {
    version = "21.0.4"
    modules = listOf("javafx.controls", "javafx.fxml")
}

dependencies {
    implementation("io.github.mkpaz:atlantafx-base:2.0.1")
    implementation("org.slf4j:slf4j-api:2.0.13")
    implementation("ch.qos.logback:logback-classic:1.5.6")
    implementation("org.xerial:sqlite-jdbc:3.46.1.0")
    implementation("org.mongodb:mongodb-driver-sync:5.1.4")
    implementation("org.kordamp.ikonli:ikonli-javafx:12.3.1")
    implementation("org.kordamp.ikonli:ikonli-feather-pack:12.3.1")
    implementation("org.fxmisc.richtext:richtextfx:0.11.2")

    testImplementation("org.junit.jupiter:junit-jupiter:5.10.2")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

application {
    mainClass.set("com.kubrik.mex.Launcher")
}

tasks.test {
    useJUnitPlatform()
}

runtime {
    options.set(listOf("--strip-debug", "--compress", "2", "--no-header-files", "--no-man-pages"))
    modules.set(listOf(
        "java.base", "java.desktop", "java.logging", "java.management",
        "java.naming", "java.net.http", "java.security.jgss", "java.sql",
        "java.xml", "jdk.crypto.ec", "jdk.unsupported"
    ))
    jpackage {
        imageName = "Mongo Explorer"
        installerName = "Mongo Explorer"
        appVersion = project.version.toString()
        jvmArgs = listOf("--add-opens=java.base/java.lang=ALL-UNNAMED")
        val os = org.gradle.internal.os.OperatingSystem.current()
        val iconBase = "${projectDir}/src/main/resources/icons/app"
        val iconFile = when {
            os.isMacOsX -> "$iconBase.icns"
            os.isWindows -> "$iconBase.ico"
            else -> "$iconBase.png"
        }
        if (file(iconFile).exists()) {
            imageOptions = listOf("--icon", iconFile)
        }
        if (os.isMacOsX) {
            installerType = "dmg"
            installerOptions = listOf("--vendor", "Example", "--mac-package-name", "Mongo Explorer")
        } else if (os.isWindows) {
            installerType = "msi"
            installerOptions = listOf("--vendor", "Example", "--win-shortcut", "--win-menu", "--win-dir-chooser")
        } else {
            installerType = "deb"
            installerOptions = listOf("--vendor", "Example", "--linux-shortcut")
        }
    }
}
