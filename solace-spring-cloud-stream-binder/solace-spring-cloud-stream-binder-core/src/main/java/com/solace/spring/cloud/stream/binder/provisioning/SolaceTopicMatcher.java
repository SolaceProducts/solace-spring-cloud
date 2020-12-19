package com.solace.spring.cloud.stream.binder.provisioning;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SolaceTopicMatcher implements Comparable<SolaceTopicMatcher> {
    private static final Pattern topicVariablesPattern = Pattern.compile("<(\\w+)>", Pattern.MULTILINE);

    private final Pattern topicPattern;
    private final List<String> variables = new ArrayList<>();
    private final String topicName;

    public SolaceTopicMatcher(String topicName) {
        this.topicName = topicName;
        if (topicName.endsWith("/>")) {
            topicName = topicName.substring(0, topicName.length() - 1) + ".*";
        }
        final Matcher matcher = topicVariablesPattern.matcher(topicName);
        while (matcher.find()) {
            for (int i = 1; i <= matcher.groupCount(); i++) {
                variables.add(matcher.group(i));
            }
        }
        topicPattern = Pattern.compile(matcher.replaceAll("([^/]+)"));
    }

    public static String replaceTopicVariablesToAsterisk(String topicName) {
        Matcher topicNameMatcher = topicVariablesPattern.matcher(topicName);
        return topicNameMatcher.replaceAll("*");
    }

    public static boolean containsVariable(String topicName) {
        return (topicVariablesPattern.matcher(topicName).find());
    }

    public String getTopicName() {
        return topicName;
    }

    public Map<String, String> match(String topicName) {
        Matcher matcher = topicPattern.matcher(topicName);
        if (matcher.find()) {
            Map<String, String> variableValues = new HashMap<>();
            for (int i = 1; i <= matcher.groupCount(); i++) {
                variableValues.put(
                        variables.get(i - 1),
                        matcher.group(i)
                    );
            }
            return variableValues;
        }

        return null;
    }

    @Override
    public int compareTo(SolaceTopicMatcher o) {
        String[] parts = getTopicName().split("/");
        String[] oParts = o.getTopicName().split("/");
        int minLength = Math.min(parts.length, oParts.length);

        for (int i = 0 ; i < minLength ; i++) {
            boolean partIsWildcard = parts[i].contains(">") || parts[i].contains("*");
            boolean oPartIsWildcard = oParts[i].contains(">") || oParts[i].contains("*");

            if (partIsWildcard && oPartIsWildcard) {
                continue;
            }

            if (partIsWildcard) {
                return 1;
            }

            if (oPartIsWildcard) {
                return -1;
            }

            int res = parts[i].compareTo(oParts[i]);
            if (res != 0) {
                return res;
            }
        }

        return parts.length < oParts.length ? 1 : -1;
    }
}
