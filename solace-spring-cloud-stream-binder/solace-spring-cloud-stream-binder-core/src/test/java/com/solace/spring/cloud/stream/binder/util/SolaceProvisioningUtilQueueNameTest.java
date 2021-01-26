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

    @Parameters(name = "topic: {0}, group: {1}, prefix: {2}, suffix: {3}, useGroup: {4} == {5}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "simple/destination",       "simpleGroup", "", null, true,                "simple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, true,                "wildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "prefix", null, true,          "prefixsimple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, true,          "prefixwildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", true,        "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", true,        "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", true,  "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", true,  "prefixwildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "", null, false,               "simple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, false,               "wildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "prefix", null, false,         "prefixsimple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, false,         "prefixwildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", false,       "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", false,       "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", false, "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", false, "prefixwildcard/_/destination/_.annoPostix" }
        });
    }

    public SolaceProvisioningUtilQueueNameTest(String destination, String groupName, String prefix,
                                               String anonymousGroupPostfix, boolean useGroupName, String expected) {
        this.destination = destination;
        this.groupName = groupName;
        this.isAnonymous = anonymousGroupPostfix != null;
        this.expected = expected;
        this.expectSuffix = anonymousGroupPostfix != null;

        this.consumerProperties = new SolaceConsumerProperties();
        this.consumerProperties.setAnonymousGroupPostfix(anonymousGroupPostfix);
        this.consumerProperties.setPrefix(prefix);
        this.consumerProperties.setUseGroupNameInQueueName(useGroupName);
    }

    @Test
    public void getQueueName() {
        if (expectSuffix) {
            assertTrue(SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                    .getConsumerGroupQueueName().startsWith(expected));
        } else {
            assertEquals(expected, SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties,
                    isAnonymous).getConsumerGroupQueueName());
        }
    }

    @Test
    public void getErrorQueueName() {
        if (expectSuffix) {
            assertTrue(SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                    .getErrorQueueName().startsWith(expected));
        } else if (!consumerProperties.isUseGroupNameInQueueName()) {
            assertEquals(expected + '.' + groupName + ".error",
                    SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                            .getErrorQueueName());
        } else {
            assertEquals(expected + ".error",
                    SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                            .getErrorQueueName());
        }
    }
}
