package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class SolaceProvisioningUtilQueueNameTest {

    private final String destination;
    private final String groupName;
    private final boolean isAnonymous;
    private final SolaceConsumerProperties consumerProperties;

    private static final Log logger = LogFactory.getLog(SolaceProvisioningUtilQueueNameTest.class);

    @Parameters(name = "dest: {0}, group: {1}, prefix: {2}, useGroup: {3}, useGroupInEQ: {4}, useFamiliarity: {5}, useDestEnc: {6}")
    public static Collection<Object[]> data() {
        List<List<Object>> testCases = new ArrayList<>();

        // destination
        testCases.add(new ArrayList<>(Collections.singletonList("simple/destination")));
        testCases.add(new ArrayList<>(Collections.singletonList("wildcard/*/destination/>")));

        // group
        {
            List<List<Object>> dupeList = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(null);
            }

            for (List<Object> testCase : dupeList) {
                testCase.add("simpleGroup");
            }
            testCases.addAll(dupeList);
        }

        // queue name prefix
        {
            List<List<Object>> dupeList1 = deepClone(testCases);
            List<List<Object>> dupeList2 = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(null);
            }

            for (List<Object> testCase : dupeList1) {
                testCase.add("custom-prefix");
            }

            for (List<Object> testCase : dupeList2) {
                testCase.add("");
            }
            testCases.addAll(dupeList1);
            testCases.addAll(dupeList2);
        }

        // useGroupNameInQueueName
        // useGroupNameInErrorQueueName
        // useFamiliarityInQueueName
        // useDestinationEncodingInQueueName
        for (int i = 0; i < 4; i++) {
            List<List<Object>> dupeList = deepClone(testCases);
            for (List<Object> testCase : testCases) {
                testCase.add(true);
            }

            for (List<Object> testCase : dupeList) {
                testCase.add(false);
            }
            testCases.addAll(dupeList);
        }

        return testCases.stream().map(List::toArray).collect(Collectors.toList());
    }

    private static List<List<Object>> deepClone(List<List<Object>> input) {
        List<List<Object>> cloned = new ArrayList<>();
        for (List<Object> nestedList : input) {
            cloned.add(new ArrayList<>(nestedList));
        }
        return cloned;
    }

    public SolaceProvisioningUtilQueueNameTest(String destination, String groupName, String prefix, boolean useGroupName,
                                               boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                                               boolean useDestinationEncoding) {
        this.destination = destination;
        this.groupName = groupName;
        this.isAnonymous = groupName == null;

        this.consumerProperties = new SolaceConsumerProperties();
        if (prefix != null) {
            this.consumerProperties.setQueueNamePrefix(prefix);
        } else {
            assertEquals("scst", this.consumerProperties.getQueueNamePrefix());
        }
        this.consumerProperties.setUseGroupNameInQueueName(useGroupName);
        this.consumerProperties.setUseGroupNameInErrorQueueName(useGroupNameInErrorQueue);
        this.consumerProperties.setUseFamiliarityInQueueName(useFamiliarity);
        this.consumerProperties.setUseDestinationEncodingInQueueName(useDestinationEncoding);
    }

    @Test
    public void getQueueName() {
        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                .getConsumerGroupQueueName();
        logger.info("Testing Queue Name: " + actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(actual, isAnonymous ? "an" : "wk", levels[levelIdx]);
            levelIdx++;
        }

        if (isAnonymous) {
            assertThat(levels[levelIdx], matchesRegex("\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b"));
            levelIdx++;
        } else {
            if (consumerProperties.isUseGroupNameInQueueName()) {
                assertEquals(groupName, levels[levelIdx]);
                levelIdx++;
            }
        }

        if (consumerProperties.isUseDestinationEncodingInQueueName()) {
            assertEquals("plain", levels[levelIdx]);
            levelIdx++;
        }

        String transformedDestination;
        if (destination.contains("*") || destination.contains(">")) {
            transformedDestination = destination.replaceAll("[*>]", "_");
        } else {
            transformedDestination = destination;
        }

        for (String destinationLevel : transformedDestination.split("/")) {
            assertEquals(destinationLevel, levels[levelIdx]);
            levelIdx++;
        }
    }

    @Test
    public void getErrorQueueName() {
        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, consumerProperties, isAnonymous)
                .getErrorQueueName();

        logger.info("Testing Error Queue Name: " + actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        assertEquals("error", levels[levelIdx]);
        levelIdx++;

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(actual, isAnonymous ? "an" : "wk", levels[levelIdx]);
            levelIdx++;
        }

        if (isAnonymous) {
            assertThat(levels[levelIdx], matchesRegex("\\b[0-9a-f]{8}\\b(?:-[0-9a-f]{4}){3}-\\b[0-9a-f]{12}\\b"));
            levelIdx++;
        } else {
            if (consumerProperties.isUseGroupNameInErrorQueueName()) {
                assertEquals(groupName, levels[levelIdx]);
                levelIdx++;
            }
        }

        if (consumerProperties.isUseDestinationEncodingInQueueName()) {
            assertEquals("plain", levels[levelIdx]);
            levelIdx++;
        }

        String transformedDestination;
        if (destination.contains("*") || destination.contains(">")) {
            transformedDestination = destination.replaceAll("[*>]", "_");
        } else {
            transformedDestination = destination;
        }

        for (String destinationLevel : transformedDestination.split("/")) {
            assertEquals(destinationLevel, levels[levelIdx]);
            levelIdx++;
        }
    }
}
