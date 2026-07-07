import com.diffplug.gradle.spotless.BaseKotlinExtension
import com.diffplug.spotless.kotlin.KtfmtStep
import com.github.benmanes.gradle.versions.updates.gradle.GradleReleaseChannel
import com.vanniktech.maven.publish.DeploymentValidation
import org.gradle.api.tasks.testing.logging.TestLogEvent

plugins {
    alias(libs.plugins.kotlin.jvm)
    alias(libs.plugins.kotlinx.serialization)
    alias(libs.plugins.versions)
    alias(libs.plugins.spotless)
    alias(libs.plugins.maven.publish)
}

group = "io.github.jckoenen"

version = "0.5.0"

repositories { mavenCentral() }

kotlin {
    jvmToolchain(21)
    explicitApi()
    compilerOptions { optIn.add("kotlinx.coroutines.ExperimentalCoroutinesApi") }
}

dependencies {
    api(libs.kotlin.stdlib)

    api(platform(libs.aws.kotlin.bom))
    api(libs.aws.kotlin.sqs)

    api(platform(libs.kotlinx.coroutines.bom))
    api(libs.kotlinx.coroutines.core)
    implementation(libs.kotlinx.coroutines.slf4j)

    implementation(libs.kotlinx.datetime)

    implementation(libs.kotlinx.serialization)

    api(platform(libs.arrow.bom))
    api(libs.arrow.core)
    implementation(libs.arrow.resilience)

    compileOnly(libs.slf4j.api)

    testImplementation(libs.kotlin.reflect)
    testImplementation(libs.logback.classic)
    testImplementation(libs.kotlinx.coroutines.test)
    testImplementation(libs.kotlinx.coroutines.debug)

    testImplementation(platform(libs.kotest.bom))
    testImplementation(libs.kotest.runner.junit5)
    testImplementation(libs.kotest.core)
    testImplementation(libs.kotest.property)
    testImplementation(libs.kotest.extensions.testcontainers)
    testImplementation(libs.kotest.extensions.arrow)
    testImplementation(libs.kotest.extensions.xml)

    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.java)
    testImplementation(libs.testcontainers.localstack)
}

tasks {
    test {
        useJUnitPlatform()
        testLogging {
            events = setOf(TestLogEvent.FAILED, TestLogEvent.PASSED, TestLogEvent.SKIPPED)
        }
        reports.junitXml.required = false
        systemProperty(
            "gradle.build.dir",
            project.layout.buildDirectory.map { it.asFile.absolutePath }.get(),
        )
    }

    dependencyUpdates {
        // https://github.com/ben-manes/gradle-versions-plugin/issues/968
        doFirst { gradle.startParameter.isParallelProjectExecutionEnabled = false }

        val stableVersion = "^[0-9,.v-]+(-r)?$".toRegex()
        val stableKeywords = listOf("release", "final", "ga")

        fun isStable(version: String): Boolean =
            stableKeywords.any { version.contains(it, ignoreCase = true) } ||
                    stableVersion.matches(version)

        gradleReleaseChannel = GradleReleaseChannel.CURRENT.id
        rejectVersionIf { isStable(candidate.version) != isStable(currentVersion) }
    }
}

spotless {
    fun BaseKotlinExtension.defaultFormat() {
        ktfmt().kotlinlangStyle().configure {
            it.setMaxWidth(120)
            it.setBlockIndent(4)
            it.setContinuationIndent(4)
            it.setRemoveUnusedImports(true)
            it.setTrailingCommaManagementStrategy(KtfmtStep.TrailingCommaManagementStrategy.NONE)
        }
        targetExcludeIfContentContains("import io.kotest.core.spec.style.")
    }

    kotlin { defaultFormat() }
    kotlinGradle { defaultFormat() }
}

mavenPublishing {
    publishToMavenCentral(automaticRelease = true, validateDeployment = DeploymentValidation.VALIDATED)
    signAllPublications()

    pom {
        name = "SQS Connector"
        description = "Utilities to connect to AWS SQS with a functional interface"
        inceptionYear = "2025"
        url = "https://github.com/jckoenen/sqs-connector"
        licenses {
            license {
                name = "The Apache License, Version 2.0"
                url = "https://www.apache.org/licenses/LICENSE-2.0.txt"
                distribution = "https://www.apache.org/licenses/LICENSE-2.0.txt"
            }
        }
        developers {
            developer {
                id = "jckoenen"
                name = "Johannes Koenen"
                url = "https://github.com/jckoenen"
            }
        }
        scm {
            url = "https://github.com/jckoenen/sqs-connector"
            connection = "scm:git:git://github.com/jckoenen/sqs-connector.git"
            developerConnection = "scm:git:ssh://git@github.com:jckoenen/sqs-connector.git"
        }
    }
}
