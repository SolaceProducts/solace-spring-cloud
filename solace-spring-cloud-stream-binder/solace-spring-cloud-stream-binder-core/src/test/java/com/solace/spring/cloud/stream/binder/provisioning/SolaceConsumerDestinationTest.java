package com.solace.spring.cloud.stream.binder.provisioning;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class SolaceConsumerDestinationTest {
    @Test
    public void testTopicMatcherSorting_theMoreSpecificShouldWinOverWildcard() {
        List<SolaceTopicMatcher> topicMatchers = new ArrayList<>();
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/indoor/<country>/<zip>/<address>"));
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/indoor/de/<zip>/<address>/<room>"));

        SolaceConsumerDestination dest = new SolaceConsumerDestination("foo", topicMatchers);

        Assert.assertEquals("sensor/humidity/indoor/de/<zip>/<address>/<room>", dest.getTopicMatcher().get(0).getTopicName());
        Assert.assertEquals("sensor/humidity/indoor/<country>/<zip>/<address>", dest.getTopicMatcher().get(1).getTopicName());
    }

    @Test
    public void testTopicMatcherSorting_theMoreSpecificShouldWinOverWildcardAsterisk() {
        List<SolaceTopicMatcher> topicMatchers = new ArrayList<>();
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/indoor/*/<zip>/<address>"));
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/indoor/de/<zip>/<address>/<room>"));

        SolaceConsumerDestination dest = new SolaceConsumerDestination("foo", topicMatchers);

        Assert.assertEquals("sensor/humidity/indoor/de/<zip>/<address>/<room>", dest.getTopicMatcher().get(0).getTopicName());
        Assert.assertEquals("sensor/humidity/indoor/*/<zip>/<address>", dest.getTopicMatcher().get(1).getTopicName());
    }

    @Test
    /**
      * Just to full fill the requirements of sort, this is not a use case
      */
    public void testTopicMatcherSorting_alphabeticOrderIfBothFragmentsAreNonWildcards() {
        List<SolaceTopicMatcher> topicMatchers = new ArrayList<>();
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/outdoor/<country>/<zip>/<address>"));
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/indoor/<country>/<zip>/<address>"));

        SolaceConsumerDestination dest = new SolaceConsumerDestination("foo", topicMatchers);

        Assert.assertEquals("sensor/humidity/indoor/<country>/<zip>/<address>", dest.getTopicMatcher().get(0).getTopicName());
        Assert.assertEquals("sensor/humidity/outdoor/<country>/<zip>/<address>", dest.getTopicMatcher().get(1).getTopicName());
    }

    @Test
    public void testTopicMatcherSorting_longerTopicsFirst() {
        List<SolaceTopicMatcher> topicMatchers = new ArrayList<>();
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/outdoor/<country>/<zip>/<address>/extraArg"));
        topicMatchers.add(new SolaceTopicMatcher("sensor/humidity/outdoor/<country>/<zip>/<address>"));

        SolaceConsumerDestination dest = new SolaceConsumerDestination("foo", topicMatchers);

        Assert.assertEquals("sensor/humidity/outdoor/<country>/<zip>/<address>/extraArg", dest.getTopicMatcher().get(0).getTopicName());
        Assert.assertEquals("sensor/humidity/outdoor/<country>/<zip>/<address>", dest.getTopicMatcher().get(1).getTopicName());
    }

}
