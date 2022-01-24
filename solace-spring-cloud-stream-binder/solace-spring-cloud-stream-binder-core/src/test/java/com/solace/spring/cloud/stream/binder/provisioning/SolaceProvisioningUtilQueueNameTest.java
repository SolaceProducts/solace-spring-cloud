package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.matchesRegex;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SolaceProvisioningUtilQueueNameTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(SolaceProvisioningUtilQueueNameTest.class);

    public static Stream<Arguments> arguments() {
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

        return testCases.stream().map(List::toArray).map(Arguments::of);
    }

    private static List<List<Object>> deepClone(List<List<Object>> input) {
        List<List<Object>> cloned = new ArrayList<>();
        for (List<Object> nestedList : input) {
            cloned.add(new ArrayList<>(nestedList));
        }
        return cloned;
    }

    @ParameterizedTest(name = "[{index}] dest={0} group={1} prefix={2} useGroup={3} useGroupInEQ={4} useFamiliarity={5} useDestEnc={6}")
    @MethodSource("arguments")
    public void getQueueName(String destination, String groupName, String prefix, boolean useGroupName,
                             boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                             boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = createConsumerProperties(prefix, useGroupName,
                useGroupNameInErrorQueue, useFamiliarity, useDestinationEncoding);
        boolean isAnonymous = groupName == null;

        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, new ExtendedConsumerProperties<>(consumerProperties), isAnonymous)
                .getConsumerGroupQueueName();
        LOGGER.info("Testing Queue Name: {}", actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(isAnonymous ? "an" : "wk", levels[levelIdx]);
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

    @ParameterizedTest(name = "[{index}] dest={0} group={1} prefix={2} useGroup={3} useGroupInEQ={4} useFamiliarity={5} useDestEnc={6}")
    @MethodSource("arguments")
    public void getErrorQueueName(String destination, String groupName, String prefix, boolean useGroupName,
                                  boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                                  boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = createConsumerProperties(prefix, useGroupName,
                useGroupNameInErrorQueue, useFamiliarity, useDestinationEncoding);
        boolean isAnonymous = groupName == null;

        String actual = SolaceProvisioningUtil.getQueueNames(destination, groupName, new ExtendedConsumerProperties<>(consumerProperties), isAnonymous)
                .getErrorQueueName();

        LOGGER.info("Testing Error Queue Name: {}", actual);

        int levelIdx = 0;
        String[] levels = actual.split("/");

        if (!consumerProperties.getQueueNamePrefix().isEmpty()) {
            assertEquals(consumerProperties.getQueueNamePrefix(), levels[levelIdx]);
            levelIdx++;
        }

        assertEquals("error", levels[levelIdx]);
        levelIdx++;

        if (consumerProperties.isUseFamiliarityInQueueName()) {
            assertEquals(isAnonymous ? "an" : "wk", levels[levelIdx]);
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

    private SolaceConsumerProperties createConsumerProperties(String prefix, boolean useGroupName,
                                                              boolean useGroupNameInErrorQueue, boolean useFamiliarity,
                                                              boolean useDestinationEncoding) {
        SolaceConsumerProperties consumerProperties = new SolaceConsumerProperties();
        if (prefix != null) {
            consumerProperties.setQueueNamePrefix(prefix);
        } else {
            assertEquals("scst", consumerProperties.getQueueNamePrefix());
        }
        consumerProperties.setUseGroupNameInQueueName(useGroupName);
        consumerProperties.setUseGroupNameInErrorQueueName(useGroupNameInErrorQueue);
        consumerProperties.setUseFamiliarityInQueueName(useFamiliarity);
        consumerProperties.setUseDestinationEncodingInQueueName(useDestinationEncoding);
        return consumerProperties;
    }
}
