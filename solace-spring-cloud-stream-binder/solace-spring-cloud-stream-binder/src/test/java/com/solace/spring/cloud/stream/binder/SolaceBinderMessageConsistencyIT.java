package com.solace.spring.cloud.stream.binder;

import static com.solace.spring.cloud.stream.binder.messaging.SolaceBinderHeaders.BATCHED_HEADERS;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.APPLICATION_MESSAGE_ID;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.CORRELATION_ID;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.DMQ_ELIGIBLE;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.HTTP_CONTENT_ENCODING;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.PRIORITY;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.REPLY_TO;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.SENDER_ID;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.SENDER_TIMESTAMP;
import static com.solace.spring.cloud.stream.binder.messaging.SolaceHeaders.USER_DATA;
import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.messaging.MessageHeaders.CONTENT_TYPE;
import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.junit.param.provider.JCSMPMessageTypeArgumentsProvider;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SimpleJCSMPEventHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.common.util.ByteArray;
import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.MapMessage;
import com.solacesystems.jcsmp.Message;
import com.solacesystems.jcsmp.SDTException;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.SDTStream;
import com.solacesystems.jcsmp.StreamMessage;
import com.solacesystems.jcsmp.TextMessage;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.User_Cos;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.AbstractListAssert;
import org.assertj.core.api.ObjectAssert;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junitpioneer.jupiter.cartesian.CartesianArgumentsSource;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.StaticMessageHeaderAccessor;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHeaders;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.util.MimeType;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class, initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
public class SolaceBinderMessageConsistencyIT {

  private static final Logger logger = LoggerFactory.getLogger(SolaceBinderMessageConsistencyIT.class);

  private static final byte[] BYTE_PAYLOAD = "myBytePayload".getBytes(StandardCharsets.UTF_8);
  private static final String STRING_PAYLOAD = "myStringPayload";

  @CartesianTest(name = "[{index}] messageType={0} batchMode={1}")
  public void testMessageForwardedConsistently(
      @CartesianArgumentsSource(JCSMPMessageTypeArgumentsProvider.class) Class<? extends Message> messageType,
      @Values(booleans = {false, true}) boolean batchMode, JCSMPSession jcsmpSession,
      SpringCloudStreamContext context, SoftAssertions softly) throws Exception {

    SolaceTestBinder binder = context.getBinder();
    DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

    String dest = RandomStringUtils.randomAlphanumeric(10);
    String group = RandomStringUtils.randomAlphanumeric(10);

    ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
    consumerProperties.setBatchMode(batchMode);
    Binding<MessageChannel> consumerBinding = binder.bindConsumer(dest, group, moduleInputChannel, consumerProperties);

    final CountDownLatch latch = new CountDownLatch(1);
    moduleInputChannel.subscribe(msg -> {
      logger.info("Received message {}", StaticMessageHeaderAccessor.getId(msg));

      AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>> payloadsAssert =
          batchMode ? softly.assertThat(msg.getPayload()).asList()
              .hasSize(consumerProperties.getExtension().getBatchMaxSize())
              : softly.assertThat(Collections.singletonList(msg.getPayload()));

      payloadsAssert.allSatisfy(payload -> {
        assertPayload(messageType, payload);
      });

      AbstractListAssert<?, List<?>, Object, ObjectAssert<Object>> headerAssert =
          batchMode ? softly.assertThat(msg.getHeaders().get(BATCHED_HEADERS)).asList()
              .hasSize(consumerProperties.getExtension().getBatchMaxSize())
              : softly.assertThat(Collections.singletonList(msg.getHeaders()));
      headerAssert.allSatisfy(headers -> {
        assertHeaderAndUserProperties((MessageHeaders) headers);
      });

      latch.countDown();
    });

    XMLMessageProducer producer = jcsmpSession.getMessageProducer(new SimpleJCSMPEventHandler());

    for (int i = 0; i < (batchMode ? consumerProperties.getExtension().getBatchMaxSize() : 1);
        i++) {
      Message orgMessage = createMessage(messageType);
      augmentWithAllHeaderAndUserProperties(orgMessage);
      producer.send(orgMessage, JCSMPFactory.onlyInstance().createTopic(dest));
    }

    assertThat(latch.await(30, TimeUnit.SECONDS)).isTrue();
    TimeUnit.SECONDS.sleep(
        1); // Give bindings a sec to finish processing successful message consume

    consumerBinding.unbind();
    producer.close();
  }

  Message createMessage(Class<? extends Message> jcsmpMessageType) throws SDTException {
    if (TextMessage.class.equals(jcsmpMessageType)) {
      TextMessage msg = JCSMPFactory.onlyInstance().createMessage(TextMessage.class);
      msg.setText(STRING_PAYLOAD);
      return msg;
    } else if (XMLContentMessage.class.equals(jcsmpMessageType)) {
      XMLContentMessage msg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
      msg.setXMLContent(STRING_PAYLOAD);
      return msg;
    } else if (BytesMessage.class.equals(jcsmpMessageType)) {
      BytesMessage msg = JCSMPFactory.onlyInstance().createMessage(BytesMessage.class);
      msg.setData(BYTE_PAYLOAD);
      return msg;
    } else if (MapMessage.class.equals(jcsmpMessageType)) {
      MapMessage msg = JCSMPFactory.onlyInstance().createMessage(MapMessage.class);
      msg.setMap(createSDTMapMsgPayload());
      return msg;
    } else if (StreamMessage.class.equals(jcsmpMessageType)) {
      StreamMessage msg = JCSMPFactory.onlyInstance().createMessage(StreamMessage.class);
      // empty
      msg.setStream(createSDTStreamMsgPayload());
      return msg;
    } else {
      throw new IllegalArgumentException("unsupported message type " + jcsmpMessageType);
    }

  }

  private SDTMap createSDTMapMsgPayload() throws SDTException {
    final SDTMap payload = JCSMPFactory.onlyInstance().createMap();
    payload.putBoolean("withBooleanValue", true);
    payload.putString("withStringValue", "stringValue");
    payload.putDouble("withDoubleValue", 1.01d);
    payload.putBytes("withBytesValue", "toByte".getBytes(StandardCharsets.UTF_8));
    payload.putInteger("withIntegerValue", 1000);
    return payload;
  }

  private SDTStream createSDTStreamMsgPayload() {
    final SDTStream payload = JCSMPFactory.onlyInstance().createStream();
    payload.writeBoolean(true);
    payload.writeString("stringValue");
    payload.writeDouble(1.01d);
    payload.writeBytes("toByte".getBytes(StandardCharsets.UTF_8));
    payload.writeInteger(1000);
    return payload;
  }

  void augmentWithAllHeaderAndUserProperties(Message msg) throws SDTException {
    final Topic someDestination = JCSMPFactory.onlyInstance().createTopic("someTopic");
    msg.setAckImmediately(true);
    msg.setApplicationMessageId("myMessageId");
    msg.setAsReplyMessage(true);
    msg.setCorrelationId("myCorrelationID");
    msg.setCos(User_Cos.USER_COS_3);
    msg.setDMQEligible(true);
    msg.setElidingEligible(true);
    msg.setExpiration(1_000_000_000L);
    msg.setHTTPContentEncoding("deflate");
    msg.setHTTPContentType("text/html; charset=utf-8");
    msg.setPriority(3);
    msg.setReplyTo(someDestination);
    msg.setSenderId("senderId");
    msg.setSenderTimestamp(10_000_000L);
    msg.setTimeToLive(1_000_000);
    msg.setUserData("someUserData".getBytes(StandardCharsets.UTF_8));

    final SDTMap userProperties = JCSMPFactory.onlyInstance().createMap();
    userProperties.putBoolean("withBooleanValue", true);
    userProperties.putString("withStringValue", "stringValue");
    userProperties.putDouble("withDoubleValue", 1.01d);
    userProperties.putBytes("withBytesValue", "toByte".getBytes(StandardCharsets.UTF_8));
    userProperties.putInteger("withIntegerValue", 1000);
    userProperties.putByte("withByteValue", Byte.MAX_VALUE);
    userProperties.putFloat("withFloatValue", Float.MAX_VALUE);
    userProperties.putCharacter("withCharacterValue", Character.MAX_VALUE);
    userProperties.putLong("withLongValue", Long.MAX_VALUE);
    userProperties.putByteArray("withByteArrayValue",
        new ByteArray("byteArrayThisTime".getBytes(StandardCharsets.UTF_8)));
    userProperties.putShort("withShortValue", Short.MAX_VALUE);
    userProperties.putDestination("withSolaceDestinationValue",
        JCSMPFactory.onlyInstance().createTopic("someTopicValue"));
    msg.setProperties(userProperties);
  }

  private void assertPayload(Class<?> messageType, Object payload) throws Exception {
    if (messageType == TextMessage.class) {
      assertThat(payload).isEqualTo(STRING_PAYLOAD);
    } else if (messageType == XMLContentMessage.class) {
      assertThat(payload).isEqualTo(STRING_PAYLOAD);
    } else if (messageType == BytesMessage.class) {
      assertThat(payload).isEqualTo(BYTE_PAYLOAD);
    } else if (messageType == MapMessage.class) {
      assertThat(payload).isEqualTo(createSDTMapMsgPayload());
    } else if (messageType == StreamMessage.class) {
      assertThat(payload).isEqualTo(createSDTStreamMsgPayload());
    } else {
      Assertions.fail("unsupported message type " + messageType);
    }
  }

  private void assertHeaderAndUserProperties(MessageHeaders msgHeaders) {
    final Topic someDestination = JCSMPFactory.onlyInstance().createTopic("someTopic");
    assertThat(msgHeaders.get(APPLICATION_MESSAGE_ID, String.class)).isEqualTo("myMessageId");
    assertThat(msgHeaders.get(CORRELATION_ID, String.class)).isEqualTo("myCorrelationID");
    assertThat(msgHeaders.get(DMQ_ELIGIBLE, Boolean.class)).isTrue();
    assertThat(msgHeaders.get(HTTP_CONTENT_ENCODING, String.class)).isEqualTo("deflate");
    assertThat(msgHeaders.get(PRIORITY, Integer.class)).isEqualTo(3);
    assertThat(msgHeaders.get(REPLY_TO, Topic.class)).isEqualTo(someDestination);
    assertThat(msgHeaders.get(SENDER_ID, String.class)).isEqualTo("senderId");
    assertThat(msgHeaders.get(SENDER_TIMESTAMP, Long.class)).isEqualTo(10_000_000L);
    assertThat(msgHeaders.get(USER_DATA, byte[].class)).isEqualTo("someUserData".getBytes(StandardCharsets.UTF_8));
    if (msgHeaders.get(CONTENT_TYPE) instanceof MimeType) {
      assertThat(msgHeaders.get(CONTENT_TYPE, MimeType.class)).isEqualTo(MimeType.valueOf("text/html; charset=utf-8"));
    } else {
      assertThat(msgHeaders.get(CONTENT_TYPE, String.class)).isEqualTo("text/html; charset=utf-8");
    }


    assertThat(msgHeaders.get("withBooleanValue", Boolean.class)).isTrue();
    assertThat(msgHeaders.get("withStringValue", String.class)).isEqualTo("stringValue");
    assertThat(msgHeaders.get("withDoubleValue", Double.class)).isEqualTo(1.01d);
    assertThat(msgHeaders.get("withIntegerValue", Integer.class)).isEqualTo(1000);
    assertThat(msgHeaders.get("withByteValue", Byte.class)).isEqualTo(Byte.MAX_VALUE);
    assertThat(msgHeaders.get("withFloatValue", Float.class)).isEqualTo(Float.MAX_VALUE);
    assertThat(msgHeaders.get("withCharacterValue", Character.class)).isEqualTo(Character.MAX_VALUE);
    assertThat(msgHeaders.get("withLongValue", Long.class)).isEqualTo(Long.MAX_VALUE);
    assertThat(msgHeaders.get("withShortValue", Short.class)).isEqualTo(Short.MAX_VALUE);
    assertThat(msgHeaders.get("withBytesValue", byte[].class)).isEqualTo("toByte".getBytes(StandardCharsets.UTF_8));
    assertThat(msgHeaders.get("withByteArrayValue", byte[].class)).isEqualTo("byteArrayThisTime".getBytes(StandardCharsets.UTF_8));
    assertThat(msgHeaders.get("withSolaceDestinationValue", Topic.class)).isEqualTo(JCSMPFactory.onlyInstance().createTopic("someTopicValue"));
  }
}
