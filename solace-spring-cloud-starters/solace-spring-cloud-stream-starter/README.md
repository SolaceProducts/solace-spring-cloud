# Spring Cloud Stream Binder for Solace PubSub+

An implementation of Spring's Cloud Stream Binder for integrating with Solace PubSub+ message brokers. The Spring Cloud Stream Binder project provides a higher-level abstraction towards messaging that standardizes the development of distributed message-based systems.

## Contents

* [Overview](#overview)
* [Spring Cloud Stream Binder](#spring-cloud-stream-binder)
* [Using it in your Application](#using-it-in-your-application)
* [Configuration Options](#configuration-options)
* [Failed Message Error Handling](#failed-message-error-handling)
* [Resources](#resources)
---

## Overview

The Solace implementation of the Spring Cloud Stream Binder maps the following concepts from Spring to Solace:

* Destinations to topic subscriptions (Source apps always send messages to a topic)
* Consumer groups to durable queues 
* Anonymous consumer groups to temporary queues (When no group is specified; used for SCS Publish-Subscribe Model) 

And internally, each consumer group queue is subscribed to at least their destination topic. So a typical message flow would then appear as follows:

1. Producer bindings publish messages to their destination topics
2. Consumer group queues receive the messages published to their destination topic
3. Consumers of a particular consumer group consume messages from their group in a round-robin fashion (by default)

Note that partitioning is not yet supported by this version of the binder.

Note that since the Binder always consumes from queues it is currently required that Assured Delivery be enabled on the Solace PubSub+ Message VPN being used (Assured Delivery is automatically enabled if using Solace Cloud.) 

Also, it will be assumed that you have a basic understanding of the Spring Cloud Stream project. If not, then please refer to [Spring's documentation](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/). For the sake of brevity, this document will solely focus on discussing components unique to Solace.

## Spring Cloud Stream Binder

This project extends the Spring Cloud Stream Binder project. If you are new to Spring Cloud Stream, [check out their documentation](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle).

The following is a brief excerpt from that document:

> Spring Cloud Stream is a framework for building message-driven microservice applications. Spring Cloud Stream builds upon Spring Boot to create standalone, production-grade Spring applications and uses Spring Integration to provide connectivity to message brokers. It provides opinionated configuration of middleware from several vendors, introducing the concepts of persistent publish-subscribe semantics, consumer groups, and partitions.

## Using it in your Application

### Updating your build

The releases from this project are hosted in [Maven Central](https://mvnrepository.com/artifact/com.solace.spring.cloud/spring-cloud-starter-stream-solace).

The easiest way to get started is to include the `spring-cloud-starter-stream-solace` in your application.

Here is how to include the spring cloud stream starter in your project using Gradle and Maven.

#### Using it with Gradle

```groovy
// Solace Spring Cloud Stream Binder
compile("com.solace.spring.cloud:spring-cloud-starter-stream-solace:1.2.+")
```

#### Using it with Maven

```xml
<!-- Solace Spring Cloud Stream Binder -->
<dependency>
  <groupId>com.solace.spring.cloud</groupId>
  <artifactId>spring-cloud-starter-stream-solace</artifactId>
  <version>1.2.+</version>
</dependency>
```

### Creating a Simple Solace Binding

For a quick example of declaring a consumer binding in your application, take a look at [Spring's introductory example](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/#spring-cloud-stream-overview-introducing).

Then for this example, an applicable Solace configuration file may look like:

```yaml
spring:
  cloud:
    stream:
      bindings:
        input:
          destination: queuename
          group: myconsumergroup

solace:
  java:
    host: tcp://192.168.133.64
    msgVpn: default
    clientUsername: default
    clientPassword: default
    connectRetries: -1
    reconnectRetries: -1
```

Notice that the latter half of this configuration actually originates from the [JCSMP Spring Boot Auto-Configuration project](https://github.com/SolaceProducts/solace-java-spring-boot#updating-your-application-properties).

## Configuration Options

Configuration of the Solace Spring Cloud Stream Binder is done through [Spring Boot's externalized configuration](https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-external-config.html). This is where users can control the binder's configuration options as well as the Solace Java API properties.

### Inherited Configuration Options

As for auto-configuration-related options required for auto-connecting to Solace message brokers, refer to the [JCSMP Spring Boot Auto-Configuration documentation](https://github.com/SolaceProducts/solace-java-spring-boot#configure-the-application-to-use-your-solace-pubsub-service-credentials).

For general binder configuration options and properties, refer to the [Spring Cloud Stream Reference Guide](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/#_configuration_options).

### Solace Binder Configuration Options

#### Solace Consumer Properties

The following properties are available for Solace consumers only and must be prefixed with `spring.cloud.stream.solace.bindings.<channelName>.consumer.`.

See [SolaceCommonProperties](../../solace-spring-cloud-stream-binder/solace-spring-cloud-stream-binder-core/src/main/java/com/solace/spring/cloud/stream/binder/properties/SolaceCommonProperties.java) and [SolaceConsumerProperties](../../solace-spring-cloud-stream-binder/solace-spring-cloud-stream-binder-core/src/main/java/com/solace/spring/cloud/stream/binder/properties/SolaceConsumerProperties.java) for the most updated list.

<dl>
    <dt>prefix</dt>
    <dd>
        <p>Naming prefix for all topics and queues.</p>
        <p>Default: ""</p>
    </dd>
    <dt>provisionDurableQueue</dt>
    <dd>
        <p>Whether to provision durable queues for non-anonymous consumer groups. This should only be set to false if you have externally pre-provisioned the required queue on the message broker.</p>
        <p>Default: true</p>
    </dd>
    <dt>addDurableQueueSubscription</dt>
    <dd>
        <p>Whether to add topic subscriptions to durable queues for non-anonymous consumer groups. This should only be set to false if you have externally pre-added the required topic subscriptions on the consumer group's queue on the message broker. This property also applies to topics added by the queueAdditionalSubscriptions property.</p>
        <p>Default: true</p>
    </dd>
    <dt>queueAccessType</dt>
    <dd>
        <p>Access type for the consumer group queue.</p>
        <p>Default: EndpointProperties.ACCESSTYPE_NONEXCLUSIVE</p>
    </dd>
    <dt>queuePermission</dt>
    <dd>
        <p>Permissions for the consumer group queue.</p>
        <p>Default: EndpointProperties.PERMISSION_CONSUME</p>
    </dd>
    <dt>queueDiscardBehaviour</dt>
    <dd>
        <p>If specified, whether to notify sender if a message fails to be enqueued to the consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueMaxMsgRedelivery</dt>
    <dd>
        <p>Sets the maximum message redelivery count on consumer group queue. (Zero means retry forever).</p>
        <p>Default: null</p>
    </dd>
    <dt>queueMaxMsgSize</dt>
    <dd>
        <p>Maximum message size for the consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueQuota</dt>
    <dd>
        <p>Message spool quota for the consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueRespectsMsgTtl</dt>
    <dd>
        <p>Whether the consumer group queue respects Message TTL.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueAdditionalSubscriptions</dt>
    <dd>
        <p>An array of additional topic subscriptions to be applied on the consumer group queue.</p>
        <p>These subscriptions may also contain wildcards.</p>
        <p>The prefix property is not applied on these subscriptions.</p>
        <p>Default: String[0]</p>
    </dd>
    <dt>anonymousGroupPostfix</dt>
    <dd>
        <p>Naming postfix for the anonymous consumer group queue.</p>
        <p>Default: "anon"</p>
    </dd>
    <dt>polledConsumerWaitTimeInMillis</dt>
    <dd>
        <p>Rate at which polled consumers will receive messages from their consumer group queue.</p>
        <p>Default: 100</p>
    </dd>
    <dt>requeueRejected</dt>
    <dd>
        <p>Whether message processing failures should be re-queued when autoBindDmq is false and after all binder-internal retries have been exhausted.</p>
        <p>Default: false</p>
    </dd>
    <dt>autoBindDmq</dt>
    <dd>
        <p>Whether to automatically create a durable dead message queue to which messages will be republished when message processing failures are encountered. Only applies once all internal retries have been exhausted.</p>
        <p>Default: false</p>
    </dd>
    <dt>provisionDmq</dt>
    <dd>
        <p>Whether to provision durable queues for DMQs when autoBindDmq is true. This should only be set to false if you have externally pre-provisioned the required queue on the message broker.</p>
        <p>Default: true</p>
    </dd>
    <dt>dmqAccessType</dt>
    <dd>
        <p>Access type for the DMQ.</p>
        <p>Default: EndpointProperties.ACCESSTYPE_NONEXCLUSIVE</p>
    </dd>
    <dt>dmqPermission</dt>
    <dd>
        <p>Permissions for the DMQ.</p>
        <p>Default: EndpointProperties.PERMISSION_CONSUME</p>
    </dd>
    <dt>dmqDiscardBehaviour</dt>
    <dd>
        <p>If specified, whether to notify sender if a message fails to be enqueued to the DMQ.</p>
        <p>Default: null</p>
    </dd>
    <dt>dmqMaxMsgRedelivery</dt>
    <dd>
        <p>Sets the maximum message redelivery count on the DMQ. (Zero means retry forever).</p>
        <p>Default: null</p>
    </dd>
    <dt>dmqMaxMsgSize</dt>
    <dd>
        <p>Maximum message size for the DMQ.</p>
        <p>Default: null</p>
    </dd>
    <dt>dmqQuota</dt>
    <dd>
        <p>Message spool quota for the DMQ.</p>
        <p>Default: null</p>
    </dd>
    <dt>dmqRespectsMsgTtl</dt>
    <dd>
        <p>Whether the DMQ respects Message TTL.</p>
        <p>Default: null</p>
    </dd>
    <dt>republishedMsgTtl</dt>
    <dd>
        <p>The number of milliseconds before republished messages are discarded or moved to a Solace-internal Dead Message Queue.</p>
        <p>Default: null</p>
    </dd>
</dl>

#### Solace Producer Properties

The following properties are available for Solace producers only and must be prefixed with `spring.cloud.stream.solace.bindings.<channelName>.producer.`.

See [SolaceCommonProperties](../../solace-spring-cloud-stream-binder/solace-spring-cloud-stream-binder-core/src/main/java/com/solace/spring/cloud/stream/binder/properties/SolaceCommonProperties.java) and [SolaceProducerProperties](../../solace-spring-cloud-stream-binder/solace-spring-cloud-stream-binder-core/src/main/java/com/solace/spring/cloud/stream/binder/properties/SolaceProducerProperties.java) for the most updated list.

<dl>
    <dt>prefix</dt>
    <dd>
        <p>Naming prefix for all topics and queues.</p>
        <p>Default: ""</p>
    </dd>
    <dt>provisionDurableQueue</dt>
    <dd>
        <p>Whether to provision durable queues for non-anonymous consumer groups. This should only be set to false if you have externally pre-provisioned the required queue on the message broker.</p>
        <p>Default: true</p>
    </dd>
    <dt>addDurableQueueSubscription</dt>
    <dd>
        <p>Whether to add topic subscriptions to durable queues for non-anonymous consumer groups. This should only be set to false if you have externally pre-added the required topic subscriptions on the consumer group's queue on the message broker. This property also applies to topics added by the queueAdditionalSubscriptions property.</p>
        <p>Default: true</p>
    </dd>
    <dt>queueAccessType</dt>
    <dd>
        <p>Access type for the required consumer group queue.</p>
        <p>Default: EndpointProperties.ACCESSTYPE_NONEXCLUSIVE</p>
    </dd>
    <dt>queuePermission</dt>
    <dd>
        <p>Permissions for the required consumer group queue.</p>
        <p>Default: EndpointProperties.PERMISSION_CONSUME</p>
    </dd>
    <dt>queueDiscardBehaviour</dt>
    <dd>
        <p>If specified, whether to notify sender if a message fails to be enqueued to the required consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueMaxMsgRedelivery</dt>
    <dd>
        <p>Sets the maximum message redelivery count on the required consumer group queue. (Zero means retry forever).</p>
        <p>Default: null</p>
    </dd>
    <dt>queueMaxMsgSize</dt>
    <dd>
        <p>Maximum message size for the required consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueQuota</dt>
    <dd>
        <p>Message spool quota for the required consumer group queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueRespectsMsgTtl</dt>
    <dd>
        <p>Whether the required consumer group queue respects Message TTL.</p>
        <p>Default: null</p>
    </dd>
    <dt>queueAdditionalSubscriptions</dt>
    <dd>
        <p>A mapping of required consumer groups to arrays of additional topic subscriptions to be applied on each consumer group's queue.</p>
        <p>These subscriptions may also contain wildcards.</p>
        <p>The prefix property is not applied on these subscriptions.</p>
        <p>Default: Empty Map&lt;String,String[]&gt;</p>
    </dd>
    <dt>msgTtl</dt>
    <dd>
        <p>The number of milliseconds before messages are discarded or moved to a Solace-internal Dead Message Queue.</p>
        <p>Default: null</p>
    </dd>
    <dt>msgInternalDmqEligible</dt>
    <dd>
        <p>The DMQ here is not those which the binder creates when autoBindDmq is enabled, but instead, refers to the <a href="https://docs.solace.com/Configuring-and-Managing/Setting-Dead-Msg-Queues.htm">DMQ defined by the Solace message broker itself</a>.</p>
        <p>Default: false</p>
    </dd>
</dl>

## Failed Message Error Handling

Spring cloud stream binders already provides a number of application-internal reprocessing strategies for failed messages during message consumption such as:

* Forwarding errors to various [Spring error message channels](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/#_application_error_handling)
* Internally re-processing the failed messages through the usage of a [retry template](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/#_retry_template)

However, after all internal error handling strategies have been exhausted, the Solace implementation of the binder would by default reject messages that consumer bindings fail to process. Though it may be desirable for these failed messages be preserved and externally re-processed, in which case this binder also provides 2 error handling strategies that consumer bindings can be configured to use:

* Re-queuing the message onto that consumer group's queue
* Re-publishing the message to another queue (a dead-message queue) for some external application/binding to process

Note that both strategies cannot be used at the same time, and that enabling both of them would result in the binder treating it as if message re-queuing was disabled. That is to say, re-publishing failed messages to a dead-message queues supersedes message re-queuing.

### Message Re-Queuing

A simple error handling strategy in which failed messages are simply re-queued onto the consumer group's queue. This is very similar to simply enabling the retry template (setting maxAttempts to a value greater than 1), but allows for the failed messages to be re-processed by the message broker.

### Dead-Message Queue Processing

First, it must be noted that the dead message queue (DMQ) that will be discussed in this section is different from the regular [Solace DMQ](https://docs.solace.com/Configuring-and-Managing/Setting-Dead-Msg-Queues.htm). In particular, the standard Solace DMQ is used for re-routing failed messages as a consequence of Solace PubSub+ messaging features such as TTL expiration or exceeding a message's max redelivery count. Whereas the purpose of a Solace binder DMQ is for re-routing messages which had been successfully consumed from the message broker, yet cannot be processed by the binder. For simplicity, in this document all mentions of the "DMQ" refers to the Solace binder DMQ.

A DMQ can be provisioned for a particular consumer group by setting the autoBindDmq consumer property to true. This DMQ is simply another durable queue which, aside from its purpose, is not much from the queue provisioned for consumer groups. These DMQs are named using a period-delimited concatenation of their consumer group name and "dmq". And like the queue used for consumer groups, their endpoint properties can be configured by means of any consumer properties whose names begin with "dmq".

Note that DMQs are not intended to be used with anonymous consumer groups. Since the names of these consumer groups, and in turn the name of their would-be DMQs, are randomly generated at runtime, it would provide little value to create bindings to these DMQs because of their unpredictable naming and temporary existence.

## Resources

For more information about Spring Cloud Streams try these resources:

- [Spring Docs - Spring Cloud Stream Reference Guide](https://docs.spring.io/spring-cloud-stream/docs/current/reference/htmlsingle/)
- [GitHub Samples - Spring Cloud Stream Sample Applications](https://github.com/spring-cloud/spring-cloud-stream-samples)
- [Github Source - Spring Cloud Stream Source Code](https://github.com/spring-cloud/spring-cloud-stream)

For more information about Solace technology in general please visit these resources:

- The Solace Developer Portal website at: http://dev.solace.com
- Understanding [Solace technology](http://dev.solace.com/tech/)
- Ask the [Solace community](http://dev.solace.com/community/)
