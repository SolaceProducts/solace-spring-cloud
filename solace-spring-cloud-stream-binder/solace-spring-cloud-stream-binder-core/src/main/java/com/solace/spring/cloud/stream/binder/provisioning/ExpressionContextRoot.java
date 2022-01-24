package com.solace.spring.cloud.stream.binder.provisioning;

import com.solace.spring.cloud.stream.binder.properties.SolaceCommonProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;

/**
 * The context used to evaluate a queue name expression (SpEL)
 */
public class ExpressionContextRoot {

    private final String group;
    private final String destination;
    private final boolean isAnonymous;
    private final Properties<?> properties;

    public ExpressionContextRoot(String physicalGroupName, String destination, boolean isAnonymous, ExtendedConsumerProperties<SolaceConsumerProperties> extendedProperties) {
        this.group = physicalGroupName;
        this.destination = destination;
        this.isAnonymous = isAnonymous;
        this.properties = new Properties<>(extendedProperties);
    }

    public ExpressionContextRoot(String groupName, String destination, ExtendedProducerProperties<SolaceProducerProperties> extendedProperties) {
        this.group = groupName;
        this.destination = destination;
        this.isAnonymous = false;
        this.properties = new Properties<>(extendedProperties);
    }

    public String getGroup() {
        return group;
    }

    public String getDestination() {
        return destination;
    }

    public boolean isAnonymous() {
        return isAnonymous;
    }

    public Properties<?> getProperties() {
        return properties;
    }

    private static class Properties<T> {

        private final SolaceCommonProperties solace;
        private final T spring;

        public Properties(T extendedProperties) {
            if (extendedProperties instanceof ExtendedConsumerProperties) {
                this.solace = (SolaceCommonProperties) ((ExtendedConsumerProperties<?>) extendedProperties).getExtension();
            } else if (extendedProperties instanceof ExtendedProducerProperties) {
                this.solace = (SolaceCommonProperties) ((ExtendedProducerProperties<?>) extendedProperties).getExtension();
            } else {
                throw new IllegalArgumentException("Unsupported argument type");
            }
            this.spring = extendedProperties;
        }

        public SolaceCommonProperties getSolace() {
            return solace;
        }

        public T getSpring() {
            return spring;
        }
    }
}
