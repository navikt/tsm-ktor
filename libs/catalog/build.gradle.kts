plugins {
    `version-catalog`
    `maven-publish`
}

version = file("../version").readText().trim()

catalog {
    versionCatalog {
        library("core", "no.nav.tsm:ktor:$version")
        library("auth", "no.nav.tsm:ktor-auth:$version")
        library("kafka", "no.nav.tsm:ktor-kafka:$version")
    }
}

publishing {
    publications {
        create<MavenPublication>("gpr") {
            from(components["versionCatalog"])
            groupId = "no.nav.tsm"
            artifactId = "ktor-version-catalog"
            version = version
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/navikt/tsm-ktor")
            credentials {
                username = "x-access-token"
                password = System.getenv("GITHUB_TOKEN")
            }
        }
    }
}
