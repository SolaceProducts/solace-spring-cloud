package com.solace.spring.cloud.stream.binder.properties;

import com.solace.spring.cloud.stream.binder.util.SmfMessageHeaderWriteCompatibility;
import com.solace.spring.cloud.stream.binder.util.SmfMessagePayloadWriteCompatibility;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;

import static org.assertj.core.api.Assertions.assertThat;

class SmfMessageWriterPropertiesTest {
	@CartesianTest
	void testCreateFromSolaceProducerProperties(@Values(booleans = {false, true}) boolean defaultValues) {
		SolaceProducerProperties producerProperties = new SolaceProducerProperties();

		if (!defaultValues) {
			producerProperties.getHeaderExclusions().add("foobar");
			producerProperties.setHeaderTypeCompatibility(SmfMessageHeaderWriteCompatibility.NATIVE_ONLY);
			producerProperties.setPayloadTypeCompatibility(SmfMessagePayloadWriteCompatibility.NATIVE_ONLY);
			producerProperties.setNonserializableHeaderConvertToString(true);
		}

		SmfMessageWriterProperties smfMessageWriterProperties = new SmfMessageWriterProperties(producerProperties);
		assertThat(smfMessageWriterProperties.getHeaderExclusions())
				.containsExactlyInAnyOrderElementsOf(producerProperties.getHeaderExclusions());
		assertThat(smfMessageWriterProperties.getHeaderTypeCompatibility())
				.isSameAs(producerProperties.getHeaderTypeCompatibility());
		assertThat(smfMessageWriterProperties.getPayloadTypeCompatibility())
				.isSameAs(producerProperties.getPayloadTypeCompatibility());
		assertThat(smfMessageWriterProperties.isNonSerializableHeaderConvertToString())
				.isEqualTo(producerProperties.isNonserializableHeaderConvertToString());
	}
}
