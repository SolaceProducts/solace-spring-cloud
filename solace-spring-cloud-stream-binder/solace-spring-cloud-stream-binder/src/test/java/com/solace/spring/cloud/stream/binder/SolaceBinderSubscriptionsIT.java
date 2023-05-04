package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solace.test.integration.semp.v2.monitor.ApiException;
import com.solace.test.integration.semp.v2.monitor.model.MonitorMsgVpnQueueSubscription;
import com.solacesystems.jcsmp.JCSMPProperties;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests addition of topic subscriptions on queues
 */
@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderSubscriptionsIT {
    public static final String DESTINATION = "destination";
    public static final String[] ADDITIONAL_SUBSCRIPTIONS = new String[]{"addSub1", "addSub2"};

    @CartesianTest
    public void testConsumerWITHAddDestinationAsSubscriptionToDurableQueue(
            @Values(booleans = {false, true}) boolean addDestinationAsSubscriptionToQueue,
            SpringCloudStreamContext context,
            SempV2Api sempV2Api) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(ADDITIONAL_SUBSCRIPTIONS);

        String group = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> consumerBinding = binder.bindConsumer(DESTINATION, group, moduleInputChannel, consumerProperties);

        String queueName = binder.getConsumerQueueName(consumerBinding);
        //Retrieve subscriptions from broker and validate they are correct
        assertActualSubscriptionsAreCorrect(context, sempV2Api, queueName, addDestinationAsSubscriptionToQueue);

        consumerBinding.unbind();
    }

    @CartesianTest
    public void testProducerWITHAddDestinationAsSubscriptionToDurableQueue(
            @Values(booleans = {false, true}) boolean addDestinationAsSubscriptionToQueue,
            SpringCloudStreamContext context,
            SempV2Api sempV2Api,
            TestInfo testInfo) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        DirectChannel moduleOutputChannel = context.createBindableChannel("output", new BindingProperties());

        String group0 = RandomStringUtils.randomAlphanumeric(10);

        ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
        assertThat(producerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        producerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        Map<String, String[]> subs = new HashMap<>();
        subs.put(group0, ADDITIONAL_SUBSCRIPTIONS);
        producerProperties.getExtension().setQueueAdditionalSubscriptions(subs);
        producerProperties.setRequiredGroups(group0);

        Binding<MessageChannel> producerBinding = binder.bindProducer(DESTINATION, moduleOutputChannel, producerProperties);

        String queueName = SolaceProvisioningUtil.getQueueName(DESTINATION, group0, producerProperties);
        assertActualSubscriptionsAreCorrect(context, sempV2Api, queueName, addDestinationAsSubscriptionToQueue);

        producerBinding.unbind();
    }

    @CartesianTest
    public void testPolledConsumerWITHAddDestinationAsSubscriptionToDurableQueue(
            @Values(booleans = {false, true}) boolean addDestinationAsSubscriptionToQueue,
            SpringCloudStreamContext context,
            SempV2Api sempV2Api) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        PollableSource<MessageHandler> moduleInputChannel = context.createBindableMessageSource("input", new BindingProperties());

        String group0 = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(ADDITIONAL_SUBSCRIPTIONS);

        Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(DESTINATION, group0,
                moduleInputChannel, consumerProperties);

        //Retrieve subscriptions from broker and validate they are correct
        String queueName = binder.getConsumerQueueName(consumerBinding);
        assertActualSubscriptionsAreCorrect(context, sempV2Api, queueName,
                addDestinationAsSubscriptionToQueue);

        consumerBinding.unbind();
    }

    @CartesianTest
    public void testAnonConsumerWITHAddDestinationAsSubscriptionToNonDurableQueue(
            @Values(booleans = {false, true}) boolean addDestinationAsSubscriptionToQueue,
            SpringCloudStreamContext context,
            SempV2Api sempV2Api) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        //Test defaults
        assertThat(consumerProperties.getExtension().isProvisionDurableQueue()).isTrue();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        //Configure consumer for this test
        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(ADDITIONAL_SUBSCRIPTIONS);

        //group=null makes it a non-durable queue
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(DESTINATION, null, moduleInputChannel, consumerProperties);

        String queueName = binder.getConsumerQueueName(consumerBinding);
        //Retrieve subscriptions from broker and validate they are correct
        assertActualSubscriptionsAreCorrect(context, sempV2Api, queueName, addDestinationAsSubscriptionToQueue);

        consumerBinding.unbind();
    }

    private static void assertActualSubscriptionsAreCorrect(SpringCloudStreamContext context,
                                                            SempV2Api sempV2Api,
                                                            String queueName,
                                                            boolean addDestinationAsSubscriptionToQueue)
            throws ApiException {
        String msgVpnName = (String) context.getJcsmpSession().getProperty(JCSMPProperties.VPN_NAME);
        Set<String> expectedSubscriptions = getExpectedQueueSubscriptions(
                sempV2Api.monitor().getMsgVpnQueue(msgVpnName, queueName, null).getData().isDurable(),
                addDestinationAsSubscriptionToQueue);
        List<MonitorMsgVpnQueueSubscription> actualSubscriptions =
                sempV2Api.monitor().getMsgVpnQueueSubscriptions(msgVpnName, queueName, null, null, null, null).getData();

        for (MonitorMsgVpnQueueSubscription actualSubscription : actualSubscriptions) {
            assertTrue(expectedSubscriptions.contains(actualSubscription.getSubscriptionTopic()), "Unexpected subscription: " + actualSubscription);
        }
        //Make sure no subscriptions are missing
        assertEquals(expectedSubscriptions.size(), actualSubscriptions.size(), "Some subscriptions are missing");
    }

    private static Set<String> getExpectedQueueSubscriptions(boolean isDurableQueue,
                                                             boolean addDestinationAsSubscriptionToQueue) {
        if (addDestinationAsSubscriptionToQueue) {
            Set<String> expectedSubscriptions = new HashSet<>();
            expectedSubscriptions.add(DESTINATION);
            expectedSubscriptions.addAll(Arrays.asList(ADDITIONAL_SUBSCRIPTIONS));
            return expectedSubscriptions;
        } else if (!isDurableQueue) {
            return new HashSet<>(Arrays.asList(ADDITIONAL_SUBSCRIPTIONS));
        } else {
            return Collections.emptySet();
        }
    }
}
