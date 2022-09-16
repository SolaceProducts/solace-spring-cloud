package com.solace.spring.cloud.stream.binder.meter;

import com.solacesystems.jcsmp.BytesMessage;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.XMLMessage;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Statistic;
import io.micrometer.core.instrument.binder.BaseUnits;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.commons.lang3.RandomStringUtils;
import org.assertj.core.api.ThrowingConsumer;
import org.junitpioneer.jupiter.cartesian.CartesianTest;
import org.junitpioneer.jupiter.cartesian.CartesianTest.Values;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.DOUBLE;

@SpringJUnitConfig(SolaceMessageMeterBinderTest.Config.class)
public class SolaceMessageMeterBinderTest {
	@CartesianTest(name = "[{index}] writeAttachment={0}, writeXmlContent={1}, writeMetadata={2}")
	public void testMessageSizeTotalMeter(@Values(booleans = {false, true}) boolean writeAttachment,
										  @Values(booleans = {false, true}) boolean writeXmlContent,
										  @Values(booleans = {false, true}) boolean writeMetadata,
										  @Autowired SolaceMessageMeterBinder solaceMessageMeterBinder,
										  @Autowired MeterRegistry meterRegistry) {
		BytesMessage message = createTestMessage(writeAttachment, writeXmlContent, writeMetadata);
		String bindingName = RandomStringUtils.randomAlphanumeric(100);
		solaceMessageMeterBinder.recordMessage(bindingName, message);

		assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_TOTAL_SIZE)
				.tag(SolaceMessageMeterBinder.TAG_NAME, bindingName)
				.meters())
				.hasSize(1)
				.first()
				.satisfies(isValidMessageSizeMeter(bindingName,
						message.getContentLength() +
								message.getAttachmentContentLength() +
								message.getBinaryMetadataContentLength(0)));
	}

	@CartesianTest(name = "[{index}] writeAttachment={0}, writeXmlContent={1}, writeMetadata={2}")
	public void testMessageSizePayloadMeter(@Values(booleans = {false, true}) boolean writeAttachment,
											@Values(booleans = {false, true}) boolean writeXmlContent,
											@Values(booleans = {false, true}) boolean writeMetadata,
											@Autowired SolaceMessageMeterBinder solaceMessageMeterBinder,
											@Autowired MeterRegistry meterRegistry) {
		BytesMessage message = createTestMessage(writeAttachment, writeXmlContent, writeMetadata);
		String bindingName = RandomStringUtils.randomAlphanumeric(100);
		solaceMessageMeterBinder.recordMessage(bindingName, message);

		assertThat(meterRegistry.find(SolaceMessageMeterBinder.METER_NAME_PAYLOAD_SIZE)
				.tag(SolaceMessageMeterBinder.TAG_NAME, bindingName)
				.meters())
				.hasSize(1)
				.first()
				.satisfies(isValidMessageSizeMeter(bindingName,
						message.getContentLength() + message.getAttachmentContentLength()));
	}

	private BytesMessage createTestMessage(boolean writeAttachment, boolean writeXmlContent, boolean writeMetadata) {
		BytesMessage message = Mockito.spy(JCSMPFactory.onlyInstance().createMessage(BytesMessage.class));
		int expectedAttachmentLength = 0;
		if (writeAttachment) {
			byte[] data = UUID.randomUUID().toString().getBytes();
			message.setData(data);
			expectedAttachmentLength = data.length;
		}
		assertThat(message)
				.extracting(XMLMessage::getAttachmentContentLength)
				.as("Message has an attachment length")
				.isEqualTo(expectedAttachmentLength);

		int expectedContentLength = 0;
		if (writeXmlContent) {
			byte[] bytes = UUID.randomUUID().toString().getBytes();
			message.writeBytes(bytes);
			expectedContentLength = bytes.length;
		}
		assertThat(message)
				.extracting(XMLMessage::getContentLength)
				.as("Message has a content length")
				.isEqualTo(expectedContentLength);

		int expectedBinaryMetadataContentLength = 0;
		if (writeMetadata) {
			expectedBinaryMetadataContentLength = 1000;
			Mockito.doReturn(expectedBinaryMetadataContentLength).when(message).getBinaryMetadataContentLength(0);
		}
		assertThat(message)
				.extracting(m -> m.getBinaryMetadataContentLength(0))
				.as("Message has binary metadata content length")
				.isEqualTo(expectedBinaryMetadataContentLength);

		return message;
	}

	private ThrowingConsumer<Meter> isValidMessageSizeMeter(String nameTagValue, double value) {
		return meter -> assertThat(meter).satisfies(
				m -> assertThat(m.getId())
						.as("Checking ID for meter %s", meter)
						.satisfies(
								meterId -> assertThat(meterId.getType()).isEqualTo(Meter.Type.DISTRIBUTION_SUMMARY),
								meterId -> assertThat(meterId.getBaseUnit()).isEqualTo(BaseUnits.BYTES),
								meterId -> assertThat(meterId.getTags())
										.hasSize(1)
										.first()
										.satisfies(
												tag -> assertThat(tag.getKey())
														.isEqualTo(SolaceMessageMeterBinder.TAG_NAME),
												tag -> assertThat(tag.getValue()).isEqualTo(nameTagValue)
										)
						),
				m -> assertThat(m.measure())
						.as("Checking measurements for meter %s", meter)
						.filteredOn(measurement -> measurement.getStatistic().equals(Statistic.TOTAL))
						.first()
						.extracting(Measurement::getValue)
						.asInstanceOf(DOUBLE)
						.isEqualTo(value)
		);
	}

	static class Config {
		@Bean
		public SimpleMeterRegistry simpleMeterRegistry() {
			return new SimpleMeterRegistry();
		}

		@Bean
		public SolaceMessageMeterBinder solaceMessageMeterBinder(List<MeterRegistry> meterRegistries) {
			SolaceMessageMeterBinder meterBinder = new SolaceMessageMeterBinder();
			meterRegistries.forEach(meterBinder::bindTo);
			return meterBinder;
		}
	}
}
