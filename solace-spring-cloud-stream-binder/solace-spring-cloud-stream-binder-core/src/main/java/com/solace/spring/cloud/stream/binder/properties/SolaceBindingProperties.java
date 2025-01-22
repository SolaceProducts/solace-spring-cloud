package com.solace.spring.cloud.stream.binder.properties;

import jakarta.validation.Valid;
import org.springframework.boot.context.properties.NestedConfigurationProperty;
import org.springframework.cloud.stream.binder.BinderSpecificPropertiesProvider;

public class SolaceBindingProperties implements BinderSpecificPropertiesProvider {

	@NestedConfigurationProperty
	@Valid
	private SolaceConsumerProperties consumer = new SolaceConsumerProperties();

	@NestedConfigurationProperty
	@Valid
	private SolaceProducerProperties producer = new SolaceProducerProperties();

	@Override
	public SolaceConsumerProperties getConsumer() {
		return consumer;
	}

	public void setConsumer(SolaceConsumerProperties consumer) {
		this.consumer = consumer;
	}

	@Override
	public SolaceProducerProperties getProducer() {
		return producer;
	}

	public void setProducer(SolaceProducerProperties producer) {
		this.producer = producer;
	}
}
