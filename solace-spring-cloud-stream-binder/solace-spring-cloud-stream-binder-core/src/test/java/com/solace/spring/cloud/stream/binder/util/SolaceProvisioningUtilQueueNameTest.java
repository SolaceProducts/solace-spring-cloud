package com.solace.spring.cloud.stream.binder.util;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SolaceProvisioningUtilQueueNameTest {

    private final String destination;
    private final String groupName;
    private final boolean isAnonymous;
    private final String expected;
    private final SolaceConsumerProperties consumerProperties;

    @Parameters(name = "topic: {0}, group: {1}, prefix: {2}, suffix: {3}, useGroup: {4}, useGroupInEQ: {5} == {6}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                { "simple/destination",       "simpleGroup", "", null, true, true,                "simple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, true, true,                "wildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "prefix", null, true, true,          "prefixsimple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, true, true,          "prefixwildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", true, true,        "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", true, true,        "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", true, true,  "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", true, true,  "prefixwildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "", null, false, true,               "simple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, false, true,               "wildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "prefix", null, false, true,         "prefixsimple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, false, true,         "prefixwildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", false, true,       "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", false, true,       "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", false, true, "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", false, true, "prefixwildcard/_/destination/_.annoPostix" },
                // ----

                { "simple/destination",       "simpleGroup", "", null, true, false,                "simple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, true, false,                "wildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "prefix", null, true, false,          "prefixsimple/destination.simpleGroup" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, true, false,          "prefixwildcard/_/destination/_.simpleGroup" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", true, false,        "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", true, false,        "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", true, false,  "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", true, false,  "prefixwildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "", null, false, false,               "simple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "", null, false, false,               "wildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "prefix", null, false, false,         "prefixsimple/destination" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", null, false, false,         "prefixwildcard/_/destination/_" },

                { "simple/destination",       "simpleGroup", "", "annoPostix", false, false,       "simple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "", "annoPostix", false, false,       "wildcard/_/destination/_.annoPostix" },

                { "simple/destination",       "simpleGroup", "prefix", "annoPostix", false, false, "prefixsimple/destination.annoPostix" },
                { "wildcard/*/destination/>", "simpleGroup", "prefix", "annoPostix", false, false, "prefixwildcard/_/destination/_.annoPostix" }
        });
    }

    public SolaceProvisioningUtilQueueNameTest(String destination, String groupName, String prefix,
                                               String anonymousGroupPostfix, boolean useGroupName,
                                               boolean useGroupNameInErrorQueue, String expected) {
        this.destination = destination;
        this.groupName = groupName;
        this.isAnonymous = anonymousGroupPostfix != null;
        this.expected = expected;

        this.consumerProperties = new SolaceConsumerProperties();
        this.consumerProperties.setAnonymousGroupPostfix(anonymousGroupPostfix);
        this.consumerProperties.setPrefix(prefix);
        this.consumerProperties.setUseGroupNameInQueueName(useGroupName);
        this.consumerProperties.setUseGroupNameInErrorQueueName(useGroupNameInErrorQueue);
    }

    @Test
    public void getQueueName() {
        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                .getConsumerGroupQueueName();
        if (isAnonymous) {
            assertThat(actual, startsWith(expected));
        } else {
            assertEquals(expected, actual);
        }
    }

    @Test
    public void getErrorQueueName() {
        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                .getErrorQueueName();
        if (isAnonymous) {
            assertThat(actual, startsWith(expected));
        } else if (!consumerProperties.isUseGroupNameInQueueName() &&
                consumerProperties.isUseGroupNameInErrorQueueName()) {
            assertEquals(expected + '.' + groupName + ".error", actual);
        } else if (consumerProperties.isUseGroupNameInQueueName() &&
                !consumerProperties.isUseGroupNameInErrorQueueName()) {
            assertEquals(expected.replace('.' + groupName, "") + ".error", actual);
        } else {
            assertEquals(expected + ".error", actual);
        }
    }
}
