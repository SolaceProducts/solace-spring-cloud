package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class SolaceProvisioningUtilQueueNameTest {

    private final String destination;
    private final String groupName;
    private final boolean isAnonymous;
    private final String expected;
    private final boolean expectSuffix;
    private final SolaceConsumerProperties consumerProperties;

    @Parameters(name = "topic: {0}, group: {1}, prefix: {2}, suffix: {3} == {4}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "simple/destination",       "simpleGroup", "", null,                "simple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "", null,                "wildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "prefix", null,          "prefixsimple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null,          "prefixwildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "", "annoPostix",        "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix",        "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix",  "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix",  "prefixwildcard/_/destination/_.annoPostix" }
        });
    }

    public SolaceProvisioningUtilQueueNameTest(String destination, String groupName, String prefix, String anonymousGroupPostfix, String expected) {
        this.destination = destination;
        this.groupName = groupName;
        this.isAnonymous = anonymousGroupPostfix != null;
        this.expected = expected;
        this.expectSuffix = anonymousGroupPostfix != null;

        this.consumerProperties = new SolaceConsumerProperties();
        this.consumerProperties.setAnonymousGroupPostfix(anonymousGroupPostfix);
        this.consumerProperties.setPrefix(prefix);
    }

    @Test
    public void getQueueName() {
        if (expectSuffix) {
            assertTrue(SolaceProvisioningUtil.getQueueName(destination, groupName, consumerProperties, isAnonymous).startsWith(expected));
        } else {
            assertEquals(expected, SolaceProvisioningUtil.getQueueName(destination, groupName, consumerProperties, isAnonymous));
        }

    }
}
