plugins {
    kotlin("jvm") version "1.9.20-Beta2"
    application
}

group = "id.au.jpg"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.jooq:jooq:3.18.6")
    implementation("org.postgresql:postgresql:42.6.0")
    implementation("com.zaxxer:HikariCP:5.0.1")
    implementation("org.reactivestreams:reactive-streams:1.0.4")

    testImplementation(kotlin("test"))
}

tasks.test {
    useJUnitPlatform()
}

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(17))
    }
}

kotlin {
    jvmToolchain(17)
}

application {
    mainClass.set("MainKt")
}