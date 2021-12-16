package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.*;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.util.MimeTypeUtils;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests addition of topic subscriptions on non-durable queues.
 * Non-Durable queues tests don't fit in SolaceBinderSubscriptionsIT since non-durable queues
 * are not returned by SEMP. This class uses a different approach to validate correctness.
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(InheritedTestsFilteredRunner.ParameterizedRunnerFactory.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderSubscriptionsOnNonDurableQueueIT extends SolaceBinderITBase {

    //Dictates whether destination subscription is added or not
    @Parameterized.Parameter(0)
    public boolean addDestinationAsSubscriptionToQueue;

    //Expected to have no effect as additional subscriptions are always added to non-durable queues
    @Parameterized.Parameter(1)
    public boolean provisionSubscriptionsToDurableQueue;

    //Specifies which subscription type is being validated (destination OR additional)
    @Parameterized.Parameter(2)
    public boolean testAdditionalSubscription;

    private static String destinationSubscription = "destination";
    private static String additionalSubscription = "additionalSubscription";

    @Parameterized.Parameters()
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false, false, false},
                {false, false, true},
                {false, true, false},
                {false, true, true},
                {true, false, false},
                {true, false, true},
                {true, true, false},
                {true, true, true}
        });
    }

    @Test
    public void testAnonConsumerWITHAddDestinationAsSubscriptionANDProvisionSubscriptionsToNonDurableQueue() throws Exception {
        SolaceTestBinder binder = getBinder();

        DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());
        DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        //Test defaults
        assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
        assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        //Configure consumer for this test
        consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(provisionSubscriptionsToDurableQueue);
        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(new String[]{additionalSubscription});

        //Producer either publishes to 'destination' or 'additional subscription'
        String producerDestination = testAdditionalSubscription ? additionalSubscription : destinationSubscription;
        Binding<MessageChannel> producerBinding = binder.bindProducer(producerDestination, moduleOutputChannel, createProducerProperties());
        //group=null makes it a non-durable queue
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(destinationSubscription, null, moduleInputChannel, consumerProperties);

        Message<?> message = MessageBuilder.withPayload("foo".getBytes())
                .setHeader(MessageHeaders.CONTENT_TYPE, MimeTypeUtils.TEXT_PLAIN_VALUE)
                .build();

        final CountDownLatch latch = new CountDownLatch(1);
        moduleInputChannel.subscribe(message1 -> {
            logger.info(String.format("Received message %s", message1));
            latch.countDown();
        });

        moduleOutputChannel.send(message);

        // Expectations:
        //      Additional subscriptions are always added on non-durable queues
        //      Destination subscription is only added when addDestinationAsSubscriptionToQueue=true
        if (addDestinationAsSubscriptionToQueue || testAdditionalSubscription) {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        } else {
            assertThat(latch.await(10, TimeUnit.SECONDS)).isFalse();

            Queue queue = JCSMPFactory.onlyInstance().createQueue(binder.getConsumerQueueName(consumerBinding));
            Topic topic = JCSMPFactory.onlyInstance().createTopic(destinationSubscription);
            logger.info(String.format("Subscribing queue %s to topic %s", queue.getName(), topic.getName()));
            jcsmpSession.addSubscription(queue, topic, JCSMPSession.WAIT_FOR_CONFIRM);

            logger.info(String.format("Sending message to destination %s", destinationSubscription));
            moduleOutputChannel.send(message);
            assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
        }
        TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

        producerBinding.unbind();
        consumerBinding.unbind();
    }
}
