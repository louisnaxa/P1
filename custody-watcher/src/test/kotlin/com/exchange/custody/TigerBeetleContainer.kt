package com.exchange.custody

import com.github.dockerjava.api.model.Bind
import com.github.dockerjava.api.model.HostConfig
import com.github.dockerjava.api.model.Volume
import com.github.dockerjava.api.async.ResultCallback
import com.github.dockerjava.api.model.PullResponseItem
import org.testcontainers.DockerClientFactory
import org.testcontainers.containers.BindMode
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import java.io.File
import java.net.InetAddress
import java.nio.file.Files
import java.time.Duration
import java.util.concurrent.TimeUnit

/**
 * Wraps the two-phase TigerBeetle startup:
 *   1. format — one-shot container, exits after writing the cluster file
 *   2. start  — long-running server, exposed on a random host port
 *
 * Mirrors the pattern used by the Go testcontainers-tigerbeetle module
 * (mkadirtan/testcontainers-tigerbeetle-go).
 *
 * CI note: Docker ≥ 25 blocks io_uring by default.
 * Both containers run with seccomp=unconfined to allow TigerBeetle's
 * io_uring usage.  On GitHub Actions (ubuntu-latest) this passes.
 * On hardened runners, the seccomp policy may need explicit allowlisting.
 */
class TigerBeetleContainer : AutoCloseable {

    companion object {
        // Must match the Java client version in build.gradle.kts
        private const val IMAGE = "ghcr.io/tigerbeetle/tigerbeetle:0.16.11"
        private const val TB_PORT = 3001
        private const val DATA_FILE = "/data/0_0.tigerbeetle"
    }

    // Host directory shared between the format container and the server container.
    private val dataDir: File = Files.createTempDirectory("tigerbeetle-test-").toFile()

    private val serverContainer: GenericContainer<*> = GenericContainer(IMAGE)
        .withFileSystemBind(dataDir.absolutePath, "/data", BindMode.READ_WRITE)
        .withCommand("start", "--addresses=0.0.0.0:$TB_PORT", DATA_FILE)
        .withExposedPorts(TB_PORT)
        .withCreateContainerCmdModifier { cmd ->
            // Append seccomp=unconfined without clobbering port bindings set by Testcontainers
            cmd.hostConfig?.withSecurityOpts(listOf("seccomp=unconfined"))
        }
        .waitingFor(Wait.forListeningPort().withStartupTimeout(Duration.ofSeconds(60)))

    fun start() {
        runFormat()
        serverContainer.start()
    }

    /**
     * Runs the format command to completion using the raw Docker client.
     * Using a raw one-shot container avoids Testcontainers' assumption that
     * containers should stay running.
     *
     * The raw client does not auto-pull images (unlike GenericContainer), so
     * we pull explicitly before creating the container.
     */
    private fun runFormat() {
        val docker = DockerClientFactory.instance().client()

        // Pull the image if not already present locally (idempotent on cache hit)
        docker.pullImageCmd(IMAGE)
            .exec(ResultCallback.Adapter<PullResponseItem>())
            .awaitCompletion(120, TimeUnit.SECONDS)

        val containerId = docker.createContainerCmd(IMAGE)
            .withCmd("format", "--cluster=0", "--replica=0", "--replica-count=1", DATA_FILE)
            .withHostConfig(
                HostConfig.newHostConfig()
                    .withBinds(Bind(dataDir.absolutePath, Volume("/data")))
                    .withSecurityOpts(listOf("seccomp=unconfined"))
            )
            .exec().id

        docker.startContainerCmd(containerId).exec()

        // Poll until the container exits (format typically completes in < 500 ms)
        val deadline = System.currentTimeMillis() + 30_000
        var running = true
        while (running && System.currentTimeMillis() < deadline) {
            Thread.sleep(200)
            running = docker.inspectContainerCmd(containerId).exec().state?.running == true
        }
        check(!running) { "TigerBeetle format container did not exit within 30 s" }

        docker.removeContainerCmd(containerId).exec()
    }

    /**
     * The address string to pass to the TigerBeetle Java client.
     *
     * TigerBeetle 0.16.x native client requires a bare IPv4 address — it does
     * not resolve hostnames.  Testcontainers may return "localhost" for the
     * container host, so we resolve it to a numeric IPv4 address here.
     */
    val address: String
        get() {
            val ip = InetAddress.getByName(serverContainer.host).hostAddress
            return "$ip:${serverContainer.getMappedPort(TB_PORT)}"
        }

    override fun close() {
        runCatching { serverContainer.stop() }
        dataDir.deleteRecursively()
    }
}
