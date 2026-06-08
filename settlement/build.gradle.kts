plugins {
    kotlin("jvm")
    kotlin("plugin.spring")
    id("org.springframework.boot")
    id("io.spring.dependency-management")
}

dependencies {
    implementation(project(":common"))

    implementation("org.springframework.boot:spring-boot-starter")
    implementation("com.fasterxml.jackson.module:jackson-module-kotlin")
    implementation("org.jetbrains.kotlin:kotlin-reflect")

    // Kafka
    implementation("org.springframework.kafka:spring-kafka")

    // TigerBeetle
    implementation("com.tigerbeetle:tigerbeetle-java:0.16.11")

    testImplementation(project(":engine"))
    testImplementation("org.springframework.boot:spring-boot-starter-test")
    testImplementation("org.assertj:assertj-core")

    // exchange-core is declared `implementation` in :engine so it does not leak to
    // our compile classpath via project-dependency resolution.  We need it explicitly
    // because MatchingEngineService.processCommand() returns CommandResultCode.
    testImplementation("com.github.exchange-core:exchange-core:exchange-core-0.5.3") {
        exclude(group = "net.openhft")
    }
    testImplementation("net.openhft:chronicle-core:2.22.35")
    testImplementation("net.openhft:chronicle-bytes:2.22.28")
    testImplementation("net.openhft:chronicle-wire:2.22.22")
    testImplementation("net.openhft:chronicle-threads:2.22.16")
    testImplementation("net.openhft:affinity:3.23.3")
    testImplementation("net.openhft:compiler:2.4.1")

    // Testcontainers — versions managed by root extra["testcontainers.version"]
    testImplementation("org.testcontainers:testcontainers")
    testImplementation("org.testcontainers:kafka")
    testImplementation("org.testcontainers:junit-jupiter")
}

// Integration tests (@Tag("integration")) require real Docker containers.
// Exclude them from the standard `test` task so `./gradlew build` stays fast.
// The `chaosTest` task runs them explicitly (also wired into the chaos CI job).
tasks.test {
    useJUnitPlatform { excludeTags("integration") }
}

tasks.register<Test>("chaosTest") {
    description = "Runs @Tag(\"integration\") chaos tests against real TigerBeetle + Kafka containers."
    group = "verification"
    useJUnitPlatform { includeTags("integration") }
    // Inherit JVM args from the root allprojects block
    jvmArgs(tasks.test.get().jvmArgs)
    testClassesDirs = sourceSets.test.get().output.classesDirs
    classpath = sourceSets.test.get().runtimeClasspath
    // Forward the DOCKER_HOST env var if set (needed on some macOS setups)
    environment("DOCKER_HOST", System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock")
}
