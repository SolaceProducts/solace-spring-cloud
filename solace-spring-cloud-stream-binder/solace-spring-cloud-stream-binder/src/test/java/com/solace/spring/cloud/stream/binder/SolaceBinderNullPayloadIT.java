package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.junit.param.provider.JCSMPMessageTypeArgumentsProvider;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
        initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderNullPayloadIT {
    private static final Logger logger = LoggerFactory.getLogger(SolaceBinderNullPayloadIT.class);

    @ParameterizedTest
    @ArgumentsSource(JCSMPMessageTypeArgumentsProvider.class)
    public void testNullPayload(Class<? extends Message> messageType, JCSMPSession jcsmpSession,
                                SpringCloudStreamContext context, SoftAssertions softly) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

        String dest = RandomStringUtils.randomAlphanumeric(10);
        String group = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(dest, group, moduleInputChannel, consumerProperties);

        final CountDownLatch latch = new CountDownLatch(1);
        moduleInputChannel.subscribe(msg -> {
            logger.info(String.format("Received message %s", msg));

            softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNotNull();
            softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class)).isTrue();

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

        XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {}

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {}
        });

        XMLMessage solMsg = JCSMPFactory.onlyInstance().createMessage(messageType);

        //Not setting payload
        producer.send(solMsg, JCSMPFactory.onlyInstance().createTopic(dest));

        assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
        TimeUnit.SECONDS.sleep(1); // Give bindings a sec to finish processing successful message consume

        consumerBinding.unbind();
        producer.close();
    }

    @ParameterizedTest
    @ArgumentsSource(JCSMPMessageTypeArgumentsProvider.class)
    public void testEmptyPayload(Class<? extends Message> messageType, JCSMPSession jcsmpSession,
                                 SpringCloudStreamContext context, SoftAssertions softly) throws Exception {
        SolaceTestBinder binder = context.getBinder();

        DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

        String dest = RandomStringUtils.randomAlphanumeric(10);
        String group = RandomStringUtils.randomAlphanumeric(10);

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        Binding<MessageChannel> consumerBinding = binder.bindConsumer(dest, group, moduleInputChannel, consumerProperties);

        final CountDownLatch latch = new CountDownLatch(1);
        moduleInputChannel.subscribe(msg -> {
            logger.info(String.format("Received message %s", msg));

            if (messageType == BytesMessage.class) {
                //LIMITATION: BytesMessage doesn't support EMPTY payloads since publishing byte[0] is received as a null payload
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD)).isNotNull();
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class)).isTrue();

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
                softly.assertThat(msg.getHeaders().get(SolaceBinderHeaders.NULL_PAYLOAD, Boolean.class)).isTrue();

                softly.assertThat(msg.getPayload() instanceof String).isTrue();
                softly.assertThat(msg.getPayload()).isEqualTo("");
            }
            latch.countDown();
        });

        XMLMessageProducer producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
            @Override
            public void responseReceivedEx(Object o) {}

            @Override
            public void handleErrorEx(Object o, JCSMPException e, long l) {}
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

        consumerBinding.unbind();
        producer.close();
    }

}
