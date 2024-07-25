package com.solace.spring.cloud.stream.binder;

import com.solace.spring.boot.autoconfigure.SolaceJavaAutoConfiguration;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension;
import com.solace.spring.cloud.stream.binder.test.spring.SpringCloudStreamContext;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.junit.jupiter.extension.PubSubPlusExtension;
import org.junit.Ignore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.ConfigDataApplicationContextInitializer;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageChannel;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.web.SpringJUnitWebConfig;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import static org.junit.Assert.assertNotNull;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

@SpringJUnitWebConfig(classes = { SolaceJavaAutoConfiguration.class },
        initializers = ConfigDataApplicationContextInitializer.class)
@ExtendWith(PubSubPlusExtension.class)
@ExtendWith(SpringCloudStreamExtension.class)
@TestPropertySource(properties = {  "management.endpoint.health.show-details=always", "management.health.binders.enabled=true" })
@EnableAutoConfiguration
public class SolaceBinderHealthIndicatorIT {

    private static final Logger logger = LoggerFactory.getLogger(SolaceBinderHealthIndicatorIT.class);

    @Autowired
    private WebApplicationContext webAppContext;

    private MockMvc mockMvc;

    @BeforeEach
    void setUp(WebApplicationContext context) {
        mockMvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Ignore("Strategy is flawed as SolaceMessageChannelBinderConfiguration.initSession() is not called when creating binder")
    void testHealthEndpoint(SpringCloudStreamContext context) throws Exception {
        assertNotNull(webAppContext);
        ResultActions result = mockMvc.perform(get("/actuator/health"));
        MvcResult mvcResult = result.andReturn();
        String content = mvcResult.getResponse().getContentAsString();
        logger.info("Health status: " + content);

        SolaceTestBinder binder = context.getBinder();
        DirectChannel moduleInputChannel = context.createBindableChannel("input", new BindingProperties());

        ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties = context.createConsumerProperties();
        Binding<MessageChannel> consumerBinding = binder.bindConsumer("dest", "group", moduleInputChannel, consumerProperties);

        logger.info("Health status with 1 binding: " + mockMvc.perform(get("/actuator/health")).andReturn().getResponse().getContentAsString());

        consumerBinding.unbind();
    }
}
