package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.config.ApiException;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueueResponse;
import com.solace.test.integration.semp.v2.config.model.ConfigMsgVpnQueueSubscription;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests addition of topic subscriptions on durable queues
 */
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(InheritedTestsFilteredRunner.ParameterizedRunnerFactory.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderSubscriptionsIT extends SolaceBinderITBase {

    @Parameter(0)
    public boolean addDestinationAsSubscriptionToQueue;

    @Parameter(1)
    public boolean provisionSubscriptionsToDurableQueue;

    @Parameter(2)
    public String[] expectedQueueSubscriptions;

    public String destination = "destination";

    public String[] additionalSubscriptions = new String[]{"addSub1", "addSub2"};

    @Parameterized.Parameters(name = "{index}:addDestSub={0},provSubsToDurableQueue={1}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][] {
                //Well known/Durable queue cases
                { false, false, new String[]{} },
                { false, true,  new String[]{"addSub1", "addSub2"} },
                { true,  false, new String[]{} }, //No subscriptions added for backward compatibility
                { true,  true,  new String[]{"destination", "addSub1", "addSub2"} },
        });
    }

    @Test
    public void testConsumerWITHAddDestinationAsSubscriptionANDProvisionSubscriptionsToDurableQueue() throws Exception {
        SolaceTestBinder binder = getBinder();

        DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(provisionSubscriptionsToDurableQueue);
        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(additionalSubscriptions);

        String group = RandomStringUtils.randomAlphanumeric(10);

        Binding<MessageChannel> consumerBinding = binder.bindConsumer(destination, group, moduleInputChannel, consumerProperties);

        String queueName = binder.getConsumerQueueName(consumerBinding);
        //Retrieve subscriptions from broker and validate they are correct
        assertActualSubscriptionsAreCorrect(queueName, expectedQueueSubscriptions);

        consumerBinding.unbind();
    }

    @Test
    public void testProducerWITHAddDestinationAsSubscriptionANDProvisionSubscriptionsToDurableQueue() throws Exception {
        SolaceTestBinder binder = getBinder();

        DirectChannel moduleOutputChannel = createBindableChannel("output", new BindingProperties());

        String group0 = RandomStringUtils.randomAlphanumeric(10);

        ExtendedProducerProperties<SolaceProducerProperties> producerProperties = createProducerProperties();
        assertThat(producerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
        assertThat(producerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        producerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(provisionSubscriptionsToDurableQueue);
        producerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        Map<String, String[]> subs = new HashMap<>();
        subs.put(group0, additionalSubscriptions);
        producerProperties.getExtension().setQueueAdditionalSubscriptions(subs);
        producerProperties.setRequiredGroups(group0);

        Binding<MessageChannel> producerBinding = binder.bindProducer(destination, moduleOutputChannel, producerProperties);

        String queueName = SolaceProvisioningUtil.getQueueName(destination, group0, producerProperties.getExtension());
        assertActualSubscriptionsAreCorrect(queueName, expectedQueueSubscriptions);

        producerBinding.unbind();
    }

    @Test
    public void testPolledConsumerWITHAddDestinationAsSubscriptionANDProvisionSubscriptionsToDurableQueue() throws Exception {
        SolaceTestBinder binder = getBinder();

        PollableSource<MessageHandler> moduleInputChannel = createBindableMessageSource("input", new BindingProperties());

        String group0 = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        assertThat(consumerProperties.getExtension().isProvisionSubscriptionsToDurableQueue()).isTrue();
        assertThat(consumerProperties.getExtension().isAddDestinationAsSubscriptionToQueue()).isTrue();

        consumerProperties.getExtension().setProvisionSubscriptionsToDurableQueue(provisionSubscriptionsToDurableQueue);
        consumerProperties.getExtension().setAddDestinationAsSubscriptionToQueue(addDestinationAsSubscriptionToQueue);
        consumerProperties.getExtension().setQueueAdditionalSubscriptions(additionalSubscriptions);

        Binding<PollableSource<MessageHandler>> consumerBinding = binder.bindPollableConsumer(destination, group0, moduleInputChannel, consumerProperties);

        //Retrieve subscriptions from broker and validate they are correct
        String queueName = binder.getConsumerQueueName(consumerBinding);
        assertActualSubscriptionsAreCorrect(queueName, expectedQueueSubscriptions);

        consumerBinding.unbind();
    }

    private void assertActualSubscriptionsAreCorrect(String queueName, String[] expectedQueueSubscriptions) throws ApiException {
        List<ConfigMsgVpnQueueSubscription> actualSubscriptions =
                sempV2Api.config().getMsgVpnQueueSubscriptions(msgVpnName, queueName, null, null, null, null).getData();
        List<String> expectedSubscriptions = Arrays.asList(expectedQueueSubscriptions);

        for (ConfigMsgVpnQueueSubscription actualSubscription : actualSubscriptions) {
            assertTrue("Unexpected subscription: " + actualSubscription, expectedSubscriptions.contains(actualSubscription.getSubscriptionTopic()));
        }
        //Make sure no subscriptions are missing
        assertEquals("Some subscriptions are missing", expectedSubscriptions.size(), actualSubscriptions.size());
    }
}
