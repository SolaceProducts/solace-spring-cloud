package com.solace.spring.cloud.stream.binder.inbound;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageConversionException;
import com.solacesystems.jcsmp.transaction.RollbackException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.provisioning.ConsumerDestination;
import org.springframework.core.AttributeAccessor;
import org.springframework.core.retry.RetryTemplate;
import org.springframework.integration.acks.AcknowledgmentCallback;
import org.springframework.integration.core.RecoveryCallback;
import org.springframework.messaging.support.MessageBuilder;

@ExtendWith(MockitoExtension.class)
class RetryableInboundXMLMessageListenerTest {

  private static final int MAX_RETRIES = 3; // 1 initial + 3 retries = 4 total attempts

  private RetryableInboundXMLMessageListener listener;
  private ThreadLocal<AttributeAccessor> attributesHolder;

  @Mock
  FlowReceiverContainer flowReceiverContainer;
  @Mock
  ConsumerDestination consumerDestination;
  @Mock
  JCSMPAcknowledgementCallbackFactory ackCallbackFactory;
  @Mock
  AcknowledgmentCallback acknowledgmentCallback;
  @Mock
  RecoveryCallback<Object> recoveryCallback;

  @BeforeEach
  void setUp() {
    attributesHolder = new ThreadLocal<>();
    when(acknowledgmentCallback.isAutoAck()).thenReturn(true);

    ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties =
        new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
    consumerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(10));

    RetryTemplate retryTemplate = new RetryTemplate();
    retryTemplate.setRetryPolicy(
        JCSMPInboundChannelAdapter.withNonRetryableExceptions(retryTemplate.getRetryPolicy()));

    listener = new RetryableInboundXMLMessageListener(
        flowReceiverContainer,
        consumerDestination,
        consumerProperties,
        null,
        msg -> {
        },
        ackCallbackFactory,
        retryTemplate,
        recoveryCallback,
        null,
        null,
        attributesHolder
    );
  }

  @Test
  void testSolaceMessageConversionException_immediateRecoveryWithNoRetries() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);

    listener.handleMessage(
        () -> {
          callCount.incrementAndGet();
          throw new SolaceMessageConversionException("non-retryable conversion failure");
        },
        msg -> {
        },
        acknowledgmentCallback,
        false
    );

    assertThat(callCount).hasValue(1);
    verify(recoveryCallback).recover(any(), any(SolaceMessageConversionException.class));
    verify(acknowledgmentCallback).acknowledge(AcknowledgmentCallback.Status.ACCEPT);
  }

  @Test
  void testRollbackException_immediateRecoveryWithNoRetries() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);

    listener.handleMessage(
        () -> MessageBuilder.withPayload("test".getBytes()).build(),
        msg -> {
          callCount.incrementAndGet();
          throw new RuntimeException(new RollbackException("non-retryable rollback"));
        },
        acknowledgmentCallback,
        false
    );

    assertThat(callCount).hasValue(1);
    verify(recoveryCallback).recover(any(), any(RuntimeException.class));
    verify(acknowledgmentCallback).acknowledge(AcknowledgmentCallback.Status.ACCEPT);
  }

  @Test
  void testRegularRuntimeException_isRetried() throws Exception {
    AtomicInteger callCount = new AtomicInteger(0);

    listener.handleMessage(
        () -> {
          callCount.incrementAndGet();
          throw new RuntimeException("retryable error");
        },
        msg -> {
        },
        acknowledgmentCallback,
        false
    );

    int expectedAttempts = MAX_RETRIES + 1;
    assertThat(callCount).hasValue(expectedAttempts);
    verify(recoveryCallback).recover(any(), any(RuntimeException.class));
    verify(acknowledgmentCallback).acknowledge(AcknowledgmentCallback.Status.ACCEPT);
  }
}