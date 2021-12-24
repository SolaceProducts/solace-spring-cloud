package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.*;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
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
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(InheritedTestsFilteredRunner.ParameterizedRunnerFactory.class)
@ContextConfiguration(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigFileApplicationContextInitializer.class)
@IgnoreInheritedTests
public class SolaceBinderNullPayloadIT extends SolaceBinderITBase {

    @Parameterized.Parameter(0)
    public Class messageType;

    @Parameterized.Parameters(name = "{0}")
    public static Collection<?> headerSets() {
        return Arrays.asList(new Object[][]{
                { TextMessage.class },
                { BytesMessage.class },
                { XMLContentMessage.class },
                { MapMessage.class },
                { StreamMessage.class }
        });
    }

    @Test
    public void testNullPayload() throws Exception {
        SolaceTestBinder binder = getBinder();

        DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

        String dest = RandomStringUtils.randomAlphanumeric(10);
        String group = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(dest, group, moduleInputChannel, consumerProperties);

        SoftAssertions softly = new SoftAssertions();

        final CountDownLatch latch = new CountDownLatch(1);
        moduleInputChannel.subscribe(msg -> {
            logger.info(String.format("Received message %s", msg));

            softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNotNull();
            softly.assertThat((boolean) msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isTrue();

            if (messageType == BytesMessage.class) {
                softly.assertThat(msg.getPayload() instanceof byte[]).isTrue();
                softly.assertThat(((byte[]) msg.getPayload()).length).isEqualTo(0);
            } else if (messageType == TextMessage.class) {
                softly.assertThat(msg.getPayload() instanceof String).isTrue();
                softly.assertThat(msg.getPayload()).isEqualTo("");
            } else if (messageType == MapMessage.class) {
                softly.assertThat(msg.getPayload() instanceof SDTMap).isTrue();
                softly.assertThat(((SDTMap) msg.getPayload()).isEmpty()).isTrue();
            } else if (messageType == StreamMessage.class) {
                softly.assertThat(msg.getPayload() instanceof SDTStream).isTrue();
                softly.assertThat(((SDTStream) msg.getPayload()).hasRemaining()).isFalse();
            }
            latch.countDown();
        });

        XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void handleError(String s, JCSMPException e, long l) {}

            @Override
            public void responseReceived(String s) {}
        });

        XMLMessage solMsg = JCSMPFactory.onlyInstance().createMessage(messageType);

        //Not setting payload
        producer.send(solMsg, JCSMPFactory.onlyInstance().createTopic(dest));

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

        softly.assertAll();
        consumerBinding.unbind();
        producer.close();
    }

    @Test
    public void testEmptyPayload() throws Exception {
        SolaceTestBinder binder = getBinder();

        DirectChannel moduleInputChannel = createBindableChannel("input", new BindingProperties());

        String dest = RandomStringUtils.randomAlphanumeric(10);
        String group = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = createConsumerProperties();
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(dest, group, moduleInputChannel, consumerProperties);

        SoftAssertions softly = new SoftAssertions();

        final CountDownLatch latch = new CountDownLatch(1);
        moduleInputChannel.subscribe(msg -> {
            logger.info(String.format("Received message %s", msg));

            if (messageType == BytesMessage.class) {
                //LIMITATION: BytesMessage doesn't support EMPTY payloads since publishing byte[0] is received as a null payload
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNotNull();
                softly.assertThat((boolean) msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isTrue();

                softly.assertThat(msg.getPayload() instanceof byte[]).isTrue();
                softly.assertThat(((byte[]) msg.getPayload()).length).isEqualTo(0);
            } else if (messageType == TextMessage.class) {
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNull();
                softly.assertThat(msg.getPayload() instanceof String).isTrue();
                softly.assertThat(msg.getPayload()).isEqualTo("");
            } else if (messageType == MapMessage.class) {
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNull();
                softly.assertThat(msg.getPayload() instanceof SDTMap).isTrue();
                softly.assertThat(((SDTMap) msg.getPayload()).isEmpty()).isTrue();
            } else if (messageType == StreamMessage.class) {
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNull();
                softly.assertThat(msg.getPayload() instanceof SDTStream).isTrue();
                softly.assertThat(((SDTStream) msg.getPayload()).hasRemaining()).isFalse();
            } else if (messageType == XMLContentMessage.class) {
                //LIMITATION: XMLContentMessage doesn't support EMPTY payloads since publishing "" is received as a null payload
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNotNull();
                softly.assertThat((boolean) msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isTrue();

                softly.assertThat(msg.getPayload() instanceof String).isTrue();
                softly.assertThat(msg.getPayload()).isEqualTo("");
            }
            latch.countDown();
        });

        XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
            @Override
            public void handleError(String s, JCSMPException e, long l) {}

            @Override
            public void responseReceived(String s) {}
        });

        XMLMessage solMsg = JCSMPFactory.onlyInstance().createMessage(messageType);
        //Setting empty payload
        if (messageType == BytesMessage.class) {
            ((BytesMessage) solMsg).setData(new byte[0]);
        } else if (messageType == TextMessage.class) {
            ((TextMessage) solMsg).setText("");
        } else if (messageType == MapMessage.class) {
            ((MapMessage) solMsg).setMap(JCSMPFactory.onlyInstance().createMap());
        } else if (messageType == StreamMessage.class) {
            ((StreamMessage) solMsg).setStream(JCSMPFactory.onlyInstance().createStream());
        } else if (messageType == XMLContentMessage.class) {
            ((XMLContentMessage) solMsg).setXMLContent("");
        }

        producer.send(solMsg, JCSMPFactory.onlyInstance().createTopic(dest));

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

        softly.assertAll();
        consumerBinding.unbind();
        producer.close();
    }

}
