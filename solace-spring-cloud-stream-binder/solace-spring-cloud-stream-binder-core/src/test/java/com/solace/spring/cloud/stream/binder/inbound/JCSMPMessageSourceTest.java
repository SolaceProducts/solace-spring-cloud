package com.solace.spring.cloud.stream.binder.inbound;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.messaging.MessagingException;

@Timeout(value = 10)
@ExtendWith(MockitoExtension.class)
class JCSMPMessageSourceTest {

  private JCSMPMessageSource messageSource;
  private ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
  @Mock private JCSMPSession session;
  @Mock private BatchCollector batchCollector;
  @Mock private EndpointProperties endpointProperties;
  @Mock private SolaceMeterAccessor solaceMeterAccessor;

  @BeforeEach
  void init() {
    SolaceConsumerDestination dest = Mockito.mock(SolaceConsumerDestination.class);
    Mockito.when(dest.getName()).thenReturn("fake/topic");

    consumerProperties = new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
    consumerProperties.populateBindingName(RandomStringUtils.randomAlphanumeric(100));

    messageSource = new JCSMPMessageSource(
        dest,
        session,
        batchCollector,
        consumerProperties,
        endpointProperties,
        solaceMeterAccessor
    );
  }

  @Test
  void test_startFailedForBadHeaderNameMapping() {
    consumerProperties.getExtension().setHeaderNameMapping(Map.of("k1", "v1", "k2", "v1"));
    assertThatThrownBy(() -> messageSource.start())
        .isInstanceOf(MessagingException.class)
        .hasMessageStartingWith(
            "Two or more keys map to the same header name in headerNameMapping");
  }
}