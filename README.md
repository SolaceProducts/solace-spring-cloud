[![Actions Status](https://github.com/SolaceProducts/solace-spring-cloud/workflows/build/badge.svg?branch=master)](https://github.com/SolaceProducts/solace-spring-cloud/actions?query=workflow%3Abuild+branch%3Amaster)

# Solace Spring Cloud

## Overview

An umbrella project containing all Solace projects for Spring Cloud.

For information about Solace Projects that are only for Spring Boot, please visit the [Solace Spring Boot](//github.com/SolaceProducts/solace-spring-boot) project.

## Table of contents
* [Repository Contents](#repository-contents)
* [Building Locally](#building-locally)
* [Release Process](#release-process)
* [Contributing](#contributing)
* [Authors](#authors)
* [License](#license)
* [Support](#support)
* [Resources](#resources)
---
## Repository Contents

### Solace Spring Cloud Bill of Materials (BOM)

The [Solace Spring Cloud BOM](./solace-spring-cloud-bom) is a POM file which defines the versions of [Solace Spring Cloud projects](#solace-spring-cloud-projects) that are compatible to a particular version of Spring Cloud.

Please consult the [Spring Cloud Compatibility Table](./solace-spring-cloud-bom/README.md#spring-cloud-version-compatibility) to determine which version of the BOM is compatible with your project.

### Solace Spring Cloud Projects

These are the projects contained within this repository:
* [Solace Spring Cloud Stream Starter](./solace-spring-cloud-starters/solace-spring-cloud-stream-starter)
* [Solace Spring Cloud Connector](./solace-spring-cloud-connector)

## Building Locally

To build the artifacts locally, simply clone this repository and run `mvn package` at the root of the project.
This will build everything.

```shell script
git clone https://github.com/SolaceProducts/solace-spring-cloud.git
cd solace-spring-cloud
mvn package # or mvn install to install them locally
```

### Maven Project Structure

```
solace-spring-cloud-build (root)
<-- solace-spring-cloud-bom
<-- solace-spring-cloud-parent 
    <-- solace-spring-cloud-connector
    <-- solace-spring-cloud-stream-binder [spring-cloud-stream-binder-solace-core]
    <-- solace-spring-cloud-stream-starter [spring-cloud-starter-stream-solace]
    <-- solace-spring-cloud-stream-autoconfigure [spring-cloud-stream-binder-solace]

Where <-- indicates the parent of the project
```

All sub-projects are included as modules of `solace-spring-cloud-build`. Running `mvn package` or `mvn install` at the root of the project will package/install all sub-projects.

#### Build Projects

These projects are used to build the `solace-spring-cloud` repository. They should not be used in your actual application.

- solace-spring-cloud-build  
This POM defines build-related plugins and profiles that are inherited by the BOM  as well as for each of the sub-projects.
The version of this POM should match the version of Spring Cloud that the build will target.  
Please do not put non-Solace-Spring-Cloud dependencies here - they belong in solace-spring-cloud-parent. The exception to this is naturally the version of Spring Cloud that this build targets.
If it shouldn't be inherited by the BOM, it doesn't go here.
- solace-spring-cloud-parent  
This POM defines common properties and dependencies for the Solace Spring Cloud projects.

### Running the Tests

Run the following command to run all the unit and integration tests:

```shell
mvn clean verify
```

#### Run Tests With An External Broker

By default, the tests requires for Docker to be installed on the host machine so that they can auto-provision a PubSub+ broker. Otherwise, the following environment variables can be set to direct the tests to use an external broker:

```
SOLACE_JAVA_HOST=tcp://localhost:55555
SOLACE_JAVA_CLIENT_USERNAME=default
SOLACE_JAVA_CLIENT_PASSWORD=default
SOLACE_JAVA_MSG_VPN=default
TEST_SOLACE_MGMT_HOST=http://localhost:8080
TEST_SOLACE_MGMT_USERNAME=admin
TEST_SOLACE_MGMT_PASSWORD=admin
```

#### Parallel Test Execution

Parallel test execution is enabled by default. Add the `-Djunit.jupiter.execution.parallel.enabled=false` option to your command to disable parallel test execution.

## Release Process

1. Update `solace-spring-boot-bom` to latest released version
1. Release
    ```shell script
    mvn -B release:prepare
    ```


## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on the process for submitting pull requests to us.

## Authors

See the list of [contributors](https://github.com/SolaceProducts/solace-spring-cloud/graphs/contributors) who participated in this project.

## License

This project is licensed under the Apache License, Version 2.0. - See the [LICENSE](LICENSE) file for details.

## Code of Conduct
[![Contributor Covenant](https://img.shields.io/badge/Contributor%20Covenant-v1.4%20adopted-ff69b4.svg)](CODE_OF_CONDUCT.md)
Please note that this project is released with a Contributor Code of Conduct. By participating in this project you agree to abide by its terms.

## Support

https://solace.com/support

## Resources

For more information about Spring Boot Auto-Configuration and Starters try these resources:
- [Spring Docs - Spring Boot Auto-Configuration](//docs.spring.io/autorepo/docs/spring-boot/current/reference/htmlsingle/#using-boot-auto-configuration)
- [Spring Docs - Developing Auto-Configuration](//docs.spring.io/autorepo/docs/spring-boot/current/reference/htmlsingle/#boot-features-developing-auto-configuration)
- [GitHub Tutorial - Master Spring Boot Auto-Configuration](//github.com/snicoll-demos/spring-boot-master-auto-configuration)

For more information about Cloud Foundry and the Solace PubSub+ service these resources:
- [Solace PubSub+ for Pivotal Cloud Foundry](http://docs.pivotal.io/solace-messaging/)
- [Cloud Foundry Documentation](http://docs.cloudfoundry.org/)
- For an introduction to Cloud Foundry: https://www.cloudfoundry.org/

For more information about Spring Cloud try these resources:
- [Spring Cloud](http://projects.spring.io/spring-cloud/)
- [Spring Cloud Stream Reference Guide](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/)
- [Spring Cloud Stream Sample Applications](https://github.com/spring-cloud/spring-cloud-stream-samples)
- [Spring Cloud Stream Source Code](https://github.com/spring-cloud/spring-cloud-stream)
- [Spring Cloud Connectors](http://cloud.spring.io/spring-cloud-connectors/)
- [Spring Cloud Connectors Docs](http://cloud.spring.io/spring-cloud-connectors/spring-cloud-connectors.html)
- [Spring Cloud Connectors GitHub](https://github.com/spring-cloud/spring-cloud-connectors)

For more information about Solace technology for Spring Boot please visit these resources:
- [Solace Spring Boot](//github.com/SolaceProducts/solace-spring-boot)

For more information about Solace technology in general please visit these resources:

- The [Solace Developer Portal](https://solace.dev)
- Ask the [Solace community](https://solace.community/)

```
.......................HELLO FROM THE OTTER SIDE...........
............................www.solace.com.................
...........................................................
...........................@@@@@@@@@@@@@@@@@@@.............
........................@@                    @@...........
.....................@      #              #     @.........
....................@       #              #      @........
.....................@          @@@@@@@@@        @.........
......................@        @@@@@@@@@@@      @..........
.....................@           @@@@@@@         @.........
.....................@              @            @.........
.....................@    \_______/   \________/ @.........
......................@         |       |        @.........
.......................@        |       |       @..........
.......................@         \_____/       @...........
....@@@@@...............@                      @...........
..@@     @...............@                     @...........
..@       @@.............@                     @...........
..@        @@............@                     @...........
..@@        @............@                     @...........
....@       @............@                      @..........
.....@@     @...........@                        @.........
.......@     @.........@                          @........
........@     @........@                           @.......
........@       @@@@@@                              @......
.........@                                            @....
.........@                                             @...
..........@@                                           @...
............@                                          @...
.............@                              @          @...
...............@                             @         @...
.................@                            @        @...
..................@                            @       @...
...................@                           @       @...
...................@                           @       @...
...................@                          @        @...
...................@                         @        @....
..................@                         @         @....
..................@                        @         @.....
..................@                       @          @.....
..................@                       @         @@.....
..................@                        @       @ @.....
..................@                          @@@@@   @.....
..................@                                  @.....
..................@                                  @.....
```
