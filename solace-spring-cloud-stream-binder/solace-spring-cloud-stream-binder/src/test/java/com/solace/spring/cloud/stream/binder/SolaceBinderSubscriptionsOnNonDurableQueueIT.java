package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.springframework.boot.test.context.ConfigFileApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.ContextConfiguration;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests addition of topic subscriptions on non-durable queues.
 * Those tests are separate from SolaceBinderSubscriptionsIT because setup and expectations are slightly different.
 * Using two classes keeps the tests cleaner.
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

    @Parameterized.Parameter(2)
    public String[] expectedQueueSubscriptions;

    private static String destinationSubscription = "destination";
    private static String additionalSubscription = "additionalSubscription";

    @Parameterized.Parameters()
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {false, false, new String[]{"additionalSubscription"}},
                {false, true, new String[]{"additionalSubscription"}},
                {true, false, new String[]{"destination", "additionalSubscription"}},
                {true, true, new String[]{"destination", "additionalSubscription"}},
        });
    }

    @Test
    public void testAnonConsumerWITHAddDestinationAsSubscriptionANDProvisionSubscriptionsToNonDurableQueue() throws Exception {
        SolaceTestBinder binder = getBinder();

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

        //group=null makes it a non-durable queue
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(destinationSubscription, null, moduleInputChannel, consumerProperties);

        String queueName = binder.getConsumerQueueName(consumerBinding);
        //Retrieve subscriptions from broker and validate they are correct
        SolaceBinderSubscriptionsIT.SubscriptionChecker.assertActualSubscriptionsAreCorrect(sempV2Api, msgVpnName, queueName, expectedQueueSubscriptions);

        consumerBinding.unbind();
    }
}
