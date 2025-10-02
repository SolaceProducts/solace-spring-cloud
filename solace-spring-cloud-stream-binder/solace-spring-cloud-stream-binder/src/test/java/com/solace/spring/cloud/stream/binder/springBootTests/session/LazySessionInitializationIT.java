package com.solace.spring.cloud.stream.binder.springBootTests.session;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;

import com.solacesystems.jcsmp.JCSMPTransportException;
import java.net.ConnectException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.cloud.stream.binder.ConsumerProperties;
import org.springframework.cloud.stream.binder.ProducerProperties;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.annotation.Bean;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

class LazySessionInitializationIT {

  @Test
  void testLazyInitialization() {
    Map<String, CountDownLatch> latches = new HashMap<>();
    Set<String> bindingNames = Set.of("sink-in-0", "otherSink-in-0", "source-out-0",
        "otherSource-out-0", "processor-in-0", "processor-out-0", "otherProcessor-in-0",
        "otherProcessor-out-0");
    bindingNames.forEach(bindingName -> latches.put(bindingName, new CountDownLatch(3)));

    try (var applicationContext = new SpringApplicationBuilder(SpringCloudStreamApp.class)
        .profiles("lazySessionInit")
        .sources(TestConfig.class)
        .run()) {

      final BindingService bindingService = applicationContext.getBean(BindingService.class);

      //Consumer Binding Creation Interception
      doAnswer(invocation -> {
        String bindingName = invocation.getArgument(1, String.class);
        RuntimeException runtimeException = invocation.getArgument(6, RuntimeException.class);
        if (runtimeException != null) {
          assertThat(runtimeException)
              .hasCauseInstanceOf(JCSMPTransportException.class)
              .hasRootCauseInstanceOf(ConnectException.class)
              .hasRootCauseMessage("Connection refused");
        }
        latches.get(bindingName).countDown();
        return invocation.callRealMethod();
      }).when(bindingService).rescheduleConsumerBinding(any(), anyString(), any(),
          any(ConsumerProperties.class), anyString(), any(), any(RuntimeException.class));

      //Producer Binding Creation Interception
      doAnswer(invocation -> {
        String bindingName = invocation.getArgument(1, String.class);
        RuntimeException runtimeException = invocation.getArgument(5, RuntimeException.class);
        if (runtimeException != null) {
          assertThat(runtimeException)
              .hasCauseInstanceOf(JCSMPTransportException.class)
              .hasRootCauseInstanceOf(ConnectException.class)
              .hasRootCauseMessage("Connection refused");
        }
        latches.get(bindingName).countDown();
        return invocation.callRealMethod();
      }).when(bindingService).rescheduleProducerBinding(any(), anyString(), any(),
          any(ProducerProperties.class), any(), any(RuntimeException.class));

      latches.forEach((bindingName, latch) -> {
        try {
          boolean completed = latch.await(60, TimeUnit.SECONDS);
          System.err.println("Latch for binding " + bindingName + " did not complete in time. "
              + latch.getCount());
          assertTrue(completed);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }

        assertHealthIsDown((WebApplicationContext) applicationContext);
      });
    }
  }

  private static void assertHealthIsDown(WebApplicationContext context) {
    checkHealth(context, "DOWN");
  }

  private static void checkHealth(WebApplicationContext context, String status) {
    var mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    await("Wait until status is " + status)
        .pollDelay(Duration.ofSeconds(5))
        .pollInterval(Duration.ofSeconds(5))
        .atMost(Duration.ofMinutes(1))
        .untilAsserted(() -> {
          mockMvc.perform(get("/actuator/health"))
              .andExpectAll(
                  jsonPath("$.status").value(status),
                  jsonPath("$.components.binders.status").value(status),
                  jsonPath("$.components.binders.components.solace1.status").value(status),
                  jsonPath("$.components.binders.components.solace2.status").value(status)
              );
        });
  }

  @TestConfiguration
  static class TestConfig {

    @Bean
    public BeanPostProcessor bindingServiceSpyPostProcessor() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String beanName) {
          if (bean instanceof BindingService) {
            return Mockito.spy(bean);
          }
          return bean;
        }
      };
    }
  }
}