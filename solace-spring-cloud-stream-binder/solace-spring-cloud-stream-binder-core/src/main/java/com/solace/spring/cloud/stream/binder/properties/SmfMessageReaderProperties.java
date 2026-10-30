package com.solace.spring.cloud.stream.binder.properties;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class SmfMessageReaderProperties {

  private Set<String> headerExclusions;
  private Map<String, String> headerNameMapping;
  private final boolean traceContextPropagationEnabled;

  public SmfMessageReaderProperties(SolaceConsumerProperties solaceConsumerProperties) {
    this.headerExclusions = new HashSet<>(Objects.requireNonNullElse(solaceConsumerProperties.getHeaderExclusions(), Set.of()));
    this.headerNameMapping = new LinkedHashMap<>(Objects.requireNonNullElse(solaceConsumerProperties.getHeaderNameMapping(), Map.of()));
    this.traceContextPropagationEnabled = solaceConsumerProperties.isTraceContextPropagation();
  }

  public Set<String> getHeaderExclusions() {
    return headerExclusions;
  }

  public Map<String, String> getHeaderNameMapping() {
    return headerNameMapping;
  }

  public boolean isTraceContextPropagationEnabled() {
    return traceContextPropagationEnabled;
  }
}