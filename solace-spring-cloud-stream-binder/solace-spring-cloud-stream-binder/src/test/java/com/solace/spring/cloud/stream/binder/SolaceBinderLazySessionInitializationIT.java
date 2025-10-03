package com.solace.spring.cloud.stream.binder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.ConsumerInfrastructureUtil;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.spring.cloud.stream.binder.util.DestinationType;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solace.test.integration.junit.jupiter.extension.ExecutorServiceExtension;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPTransportException;
import java.net.ConnectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.BinderException;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.cloud.stream.provisioning.ProvisioningException;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

@SpringJUnitConfig(classes = SolaceJavaAutoConfiguration.class,
    initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(ExecutorServiceExtension.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
class SolaceBinderLazySessionInitializationIT {

  private JCSMPProperties jcsmpProperties;

  @BeforeEach
  void setUp(JCSMPProperties jcsmpProperties) {
    this.jcsmpProperties = jcsmpProperties;
  }

  @CartesianTest(name = "[{index}] channelType={0}")
  @Execution(ExecutionMode.CONCURRENT)
  <T> void testLazySessionInitializationThrowsExceptionDuringStartConsumerBinding(
      @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
      SpringCloudStreamContext context) throws Exception {
    jcsmpProperties.setProperty("host", "localhost:12345"); //Set invalid host:port to ensure lazy session creation throws exception
    context.setJcsmpProperties(jcsmpProperties); //Set invalid properties in context
    SolaceTestBinder binder = context.getBinder();
    ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);
    T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
    ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
    consumerProperties.getExtension().setProvisionDurableQueue(false);
    consumerProperties.setAutoStartup(false);

    try {
      consumerInfrastructureUtil.createBinding(binder, "destination0", "group0", moduleInputChannel, consumerProperties);
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(channelType == PollableSource.class ? RuntimeException.class : BinderException.class)
          .hasCauseInstanceOf(JCSMPTransportException.class)
          .hasRootCauseInstanceOf(ConnectException.class)
          .rootCause()
          .hasMessageContaining("Connection refused");
    }
  }

  @CartesianTest(name = "[{index}] channelType={0} endpointType={1} autoStartup={2}")
  @Execution(ExecutionMode.CONCURRENT)
  <T> void testLazySessionInitializationThrowsExceptionDuringConsumerEndpointProvisioning(
      @Values(classes = {DirectChannel.class, PollableSource.class}) Class<T> channelType,
      @CartesianTest.Enum(EndpointType.class) EndpointType endpointType,
      @Values(booleans = {false, true}) boolean autoStartup,
      SpringCloudStreamContext context) throws Exception {
    jcsmpProperties.setProperty("host", "localhost:12345"); //Set invalid host:port to ensure lazy session creation throws exception
    context.setJcsmpProperties(jcsmpProperties); //Set invalid properties in context
    SolaceTestBinder binder = context.getBinder();
    ConsumerInfrastructureUtil<T> consumerInfrastructureUtil = context.createConsumerInfrastructureUtil(channelType);
    T moduleInputChannel = consumerInfrastructureUtil.createChannel("input", new BindingProperties());
    ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
    consumerProperties.setAutoStartup(autoStartup);
    consumerProperties.getExtension().setProvisionDurableQueue(true);
    consumerProperties.getExtension().setEndpointType(endpointType);

    try {
      consumerInfrastructureUtil.createBinding(binder, "destination0", "group0", moduleInputChannel, consumerProperties);
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(ProvisioningException.class)
          .hasCauseInstanceOf(JCSMPTransportException.class)
          .hasRootCauseInstanceOf(ConnectException.class)
          .rootCause()
          .hasMessageContaining("Connection refused");
    }
  }

  @CartesianTest(name = "[{index}] destinationType={0}")
  @Execution(ExecutionMode.CONCURRENT)
  <T> void testLazySessionInitializationThrowsExceptionDuringStartPublisherBinding(
      @CartesianTest.Enum(DestinationType.class) DestinationType destinationType,
      SpringCloudStreamContext context, TestInfo testInfo) throws Exception {
    jcsmpProperties.setProperty("host", "localhost:12345"); //Set invalid host:port to ensure lazy session creation throws exception
    context.setJcsmpProperties(jcsmpProperties); //Set invalid properties in context
    SolaceTestBinder binder = context.getBinder();
    ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
    producerProperties.getExtension().setDestinationType(destinationType);
    producerProperties.getExtension().setProvisionDurableQueue(false);
    producerProperties.setAutoStartup(false);
    BindingProperties producerBindingProperties = new BindingProperties();
    producerBindingProperties.setProducer(producerProperties);

    DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

    try {
      binder.bindProducer("destination0", moduleOutputChannel, producerProperties);
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(BinderException.class)
          .hasCauseInstanceOf(JCSMPTransportException.class)
          .hasRootCauseInstanceOf(ConnectException.class)
          .rootCause()
          .hasMessageContaining("Connection refused");
    }
  }

  @CartesianTest(name = "[{index}] destinationType={0}")
  @Execution(ExecutionMode.CONCURRENT)
  <T> void testLazySessionInitializationThrowsExceptionDuringPublisherEndpointProvisioning(
      @Values(strings = {"QUEUE"}) String destinationType, SpringCloudStreamContext context,
      TestInfo testInfo) throws Exception {
    jcsmpProperties.setProperty("host", "localhost:12345"); //Set invalid host:port to ensure lazy session creation throws exception
    context.setJcsmpProperties(jcsmpProperties); //Set invalid properties in context
    SolaceTestBinder binder = context.getBinder();
    ExtendedProducerProperties<SolaceProducerProperties> producerProperties = context.createProducerProperties(testInfo);
    producerProperties.getExtension().setDestinationType(DestinationType.valueOf(destinationType));
    producerProperties.getExtension().setProvisionDurableQueue(true);
    producerProperties.setAutoStartup(false);
    BindingProperties producerBindingProperties = new BindingProperties();
    producerBindingProperties.setProducer(producerProperties);

    DirectChannel moduleOutputChannel = context.createBindableChannel("output", producerBindingProperties);

    try {
      binder.bindProducer("destination0", moduleOutputChannel, producerProperties);
    } catch (Exception e) {
      assertThat(e)
          .isInstanceOf(ProvisioningException.class)
          .hasCauseInstanceOf(JCSMPTransportException.class)
          .hasRootCauseInstanceOf(ConnectException.class)
          .rootCause()
          .hasMessageContaining("Connection refused");
    }
  }
}