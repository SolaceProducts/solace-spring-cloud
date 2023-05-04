# Solace Spring Cloud Bill of Materials (BOM)

## Contents

* [Overview](#overview)
* [Spring Boot Version Compatibility](#spring-cloud-version-compatibility)
* [Including the BOM](#including-the-bom)

## Overview

The Solace Spring Cloud Bill of Materials (BOM) is a POM file which defines the versions of Solace Spring Cloud projects that are compatible to a particular version of Spring Cloud.

Note that since Spring Cloud depends on Spring Boot, the Solace Spring Boot BOM will be included by default by this BOM.

## Spring Cloud Version Compatibility

Consult the table below to determine which version of the BOM you need to use:

| Spring Cloud | Solace Spring Cloud BOM    | Spring Boot |
|--------------|----------------------------|-------------|
| Hoxton.SR1   | 1.0.0                      | 2.2.x       |
| Hoxton.SR6   | 1.1.0                      | 2.3.x       |
| 2020.0.1     | 2.0.0, 2.1.0, 2.2.0, 2.2.1 | 2.4.x       |
| 2021.0.1     | 2.3.0, 2.3.1, 2.3.2        | 2.6.x       |
| 2021.0.4     | 2.4.0                      | 2.7.x       |
| 2021.0.6     | 2.5.0                      | 2.7.x       |
| 2022.0.2     | 3.0.0                      | 3.0.x       |

## Including the BOM

In addition to showing how to include the BOM, the following snippets also shows how to use "version-less" Solace dependencies (`spring-cloud-starter-stream-solace` in this case) when using the BOM.

### Using it with Maven
```xml
<dependencyManagement>
    <dependencies>
        <dependency>
            <groupId>com.solace.spring.cloud</groupId>
            <artifactId>solace-spring-cloud-bom</artifactId>
            <version>3.0.0</version>
            <type>pom</type>
            <scope>import</scope>
        </dependency>
    </dependencies>
</dependencyManagement>

<dependencies>
    <dependency>
        <groupId>com.solace.spring.cloud</groupId>
        <artifactId>spring-cloud-starter-stream-solace</artifactId>
    </dependency>
</dependencies>
```

### Using it with Gradle 4
```groovy
buildscript {
    dependencies {
        classpath 'io.spring.gradle:dependency-management-plugin:1.0.8.RELEASE'
    }
}

/* Gradle 4 isn't natively compatible with Maven BOMs. Need to use the Spring's dependency management plugin. */
apply plugin: 'io.spring.dependency-management'

dependencyManagement {
    imports {
        mavenBom "com.solace.spring.cloud:solace-spring-cloud-bom:3.0.0"
    }
}

dependencies {
    compile("com.solace.spring.cloud:spring-cloud-starter-stream-solace")
}
```

### Using it with Gradle 5
```groovy
dependencies {
    implementation(platform("com.solace.spring.cloud:solace-spring-cloud-bom:3.0.0"))
    implementation("com.solace.spring.cloud:spring-cloud-starter-stream-solace")
}
```
