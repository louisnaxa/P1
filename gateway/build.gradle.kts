plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter-web")
    implementation("org.springframework.boot:spring-boot-starter-websocket")
    implementation("org.springframework.boot:spring-boot-starter-jdbc")
    implementation("org.springframework.boot:spring-boot-starter-security")
    implementation("org.springframework.boot:spring-boot-starter-oauth2-resource-server")
    runtimeOnly("org.postgresql:postgresql")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")
    implementation("com.google.guava:guava:33.3.1-jre")

    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.springframework.kafka:spring-kafka-test")
    testImplementation("org.springframework.security:spring-security-test")
    testImplementation("org.mockito.kotlin:mockito-kotlin:5.4.0")
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.postgresql:postgresql")
}

// Fast unit tests: exclude @Tag("integration") and @Tag("transfer") so Docker is never required
// locally or in the build job.
tasks.test {
    useJUnitPlatform { excludeTags("integration", "transfer") }
}

// Transfer-guard integration tests: real PostgreSQL via Testcontainers.
// Run: ./gradlew :gateway:transferTest
tasks.register<Test>("transferTest") {
    description = "Runs @Tag(\"transfer\") TransferGuard integration tests against real PostgreSQL."
    group = "verification"
    useJUnitPlatform { includeTags("transfer") }
    jvmArgs(tasks.test.get().jvmArgs ?: emptyList<String>())
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock")
    testLogging {
        events("passed", "failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}

// Account-status integration tests: real PostgreSQL via Testcontainers.
// Run: ./gradlew :gateway:statusTest
tasks.register<Test>("statusTest") {
    description = "Runs @Tag(\"integration\") account-status tests against real PostgreSQL."
    group = "verification"
    useJUnitPlatform { includeTags("integration") }
    jvmArgs(tasks.test.get().jvmArgs ?: emptyList<String>())
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock")
    testLogging {
        events("passed", "failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showExceptions = true
        showCauses = true
        showStackTraces = true
    }
}
