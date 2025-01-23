package com.solace.spring.cloud.stream.binder.springBootTests.multibinder;

import com.solace.test.integration.testcontainer.PubSubPlusContainer;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishCorrelatingEventHandler;
import com.solacesystems.jcsmp.XMLMessageProducer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


/**
 * Note: not using the PubSubPlusExtension as it appears to trigger after Spring Boot Application is started.
 * Left as an exercise to convert to it if possible. This would prevent a second broker from being started.
 */
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles("multibinder")
@DirtiesContext //Ensures all listeners are stopped
public class MultiBinderIT {
    private static final PubSubPlusContainer container = new PubSubPlusContainer();
    private static final JCSMPSession jcsmpSession;
    private static final XMLMessageProducer producer;
    private static final String QUEUE_NAME_PREFIX = "scst/wk/myConsumerGroup/plain/";
    private static final String QUEUE_NAME_1 = "MultiBinder/Queue/1";
    static {
        container.start();
        JCSMPProperties props = new JCSMPProperties(container.getHost(),
                container.getMappedPort(PubSubPlusContainer.Port.SMF.getInternalPort()),
                container.getAdminUsername(), container.getAdminPassword(), null);
        try {
            jcsmpSession = JCSMPFactory.onlyInstance().createSession(props);
            jcsmpSession.connect();
            producer = jcsmpSession.getMessageProducer(new JCSMPStreamingPublishCorrelatingEventHandler() {
                @Override
                public void responseReceivedEx(Object o) { }

                @Override
                public void handleErrorEx(Object o, JCSMPException e, long l) { }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed setup", e);
        }
    }

    @AfterAll
    public static void close() {
        if (jcsmpSession != null) {
            jcsmpSession.closeSession();
        }
    }

    @DynamicPropertySource
    static void registerPgProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.cloud.stream.binders.solace1.environment.solace.java.host",
                () -> container.getOrigin(PubSubPlusContainer.Port.SMF));
        registry.add("spring.cloud.stream.binders.solace2.environment.solace.java.host",
                () -> container.getOrigin(PubSubPlusContainer.Port.SMF));
    }

    @Test
    public void checkHealthOfMultipleSolaceBinders(@Autowired MockMvc mvc) throws Exception {
        mvc.perform(get("/actuator/health"))
                .andExpectAll(
                        status().isOk(),
                        jsonPath("components.binders.components.solace1").exists(),
                        jsonPath("components.binders.components.solace1.status").value("UP"),
                        jsonPath("components.binders.components.solace2").exists(),
                        jsonPath("components.binders.components.solace2.status").value("UP")
                );
    }

    @Test
    public void checkSolaceMetricsAreExposed(@Autowired MockMvc mvc) throws Exception {
        //Send a message to activate metrics
        producer.send(JCSMPFactory.onlyInstance().createBytesXMLMessage(),
                JCSMPFactory.onlyInstance().createQueue(QUEUE_NAME_PREFIX + QUEUE_NAME_1));

        await().until(() -> {
                mvc.perform(get("/actuator/metrics"))
                    .andExpectAll(
                        jsonPath("names", Matchers.hasItem("solace.message.size.payload")),
                        jsonPath("names", Matchers.hasItem("solace.message.size.total")));
                return true;
            }
        );
    }

}
