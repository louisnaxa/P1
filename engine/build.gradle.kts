plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // exchange-core matching engine (exclude old OpenHFT, override with Java 17+ versions)
    implementation("com.github.exchange-core:exchange-core:exchange-core-0.5.3") {
        exclude(group = "net.openhft")
    }
    implementation("net.openhft:chronicle-core:2.22.35")
    implementation("net.openhft:chronicle-bytes:2.22.28")
    implementation("net.openhft:chronicle-wire:2.22.22")
    implementation("net.openhft:chronicle-threads:2.22.16")
    implementation("net.openhft:affinity:3.23.3")
    implementation("net.openhft:compiler:2.4.1")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.assertj:assertj-core")
}
