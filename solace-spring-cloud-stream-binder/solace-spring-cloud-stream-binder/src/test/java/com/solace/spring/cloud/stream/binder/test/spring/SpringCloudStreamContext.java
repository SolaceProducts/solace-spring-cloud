package com.solace.spring.cloud.stream.binder.test.spring;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.JCSMPSession;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.MessageHandler;

import java.util.Objects;

/**
 * <p>Spring Cloud Stream Context.</p>
 * <p>Note: Parent class {@link PartitionCapableBinderTests} also inherits some unit tests. In general, do not
 * subclass this class directly but instead use
 * {@link com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension
 * SpringCloudStreamExtension} to inject this
 * context into tests.</p>
 * @see com.solace.spring.cloud.stream.binder.test.junit.extension.SpringCloudStreamExtension SpringCloudStreamExtension
 */
public class SpringCloudStreamContext extends PartitionCapableBinderTests<SolaceTestBinder,
		ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>>
		implements ExtensionContext.Store.CloseableResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(SpringCloudStreamContext.class);
	private JCSMPSession jcsmpSession;

	public SpringCloudStreamContext(JCSMPSession jcsmpSession) {
		this.jcsmpSession = Objects.requireNonNull(jcsmpSession);
	}

	/**
	 * Should only be used by subclasses.
	 */
	protected SpringCloudStreamContext() {
		this.jcsmpSession = null;
	}

	@Override
	protected boolean usesExplicitRouting() {
		return true;
	}

	@Override
	protected String getClassUnderTestName() {
		return this.getClass().getSimpleName();
	}

	@Override
	public SolaceTestBinder getBinder() {
		if (testBinder == null) {
			if (jcsmpSession == null || jcsmpSession.isClosed()) {
				throw new IllegalStateException("JCSMPSession cannot be null or closed");
			}
			logger.info("Creating new test binder");
			testBinder = new SolaceTestBinder(jcsmpSession);
		}
		return testBinder;
	}

	@Override
	public ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties() {
		return createConsumerProperties(true);
	}

	public ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties(boolean useDefaultOverrides) {
		ExtendedConsumerProperties<SolaceConsumerProperties> properties = new ExtendedConsumerProperties<>(
				new SolaceConsumerProperties());

		if (useDefaultOverrides) {
			// Disable timeout for batch messaging consistency
			properties.getExtension().setBatchTimeout(0);
		}
		return properties;
	}

	@Override
	public ExtendedProducerProperties<SolaceProducerProperties> createProducerProperties(TestInfo testInfo) {
		return new ExtendedProducerProperties<>(new SolaceProducerProperties());
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}

	@Override
	public void binderBindUnbindLatency() throws InterruptedException {
		super.binderBindUnbindLatency();
	}

	@Override
	public DirectChannel createBindableChannel(String channelName, BindingProperties bindingProperties) throws Exception {
		return super.createBindableChannel(channelName, bindingProperties);
	}

	@Override
	public DirectChannel createBindableChannel(String channelName, BindingProperties bindingProperties, boolean inputChannel) throws Exception {
		return super.createBindableChannel(channelName, bindingProperties, inputChannel);
	}

	@Override
	public DefaultPollableMessageSource createBindableMessageSource(String bindingName, BindingProperties bindingProperties) throws Exception {
		return super.createBindableMessageSource(bindingName, bindingProperties);
	}

	@Override
	public String getDestinationNameDelimiter() {
		return super.getDestinationNameDelimiter();
	}

	@Override
	public void close() {
		if (testBinder != null) {
			LOGGER.info("Destroying binder");
			testBinder.getBinder().destroy();
			testBinder = null;
		}
	}

	public <T extends AbstractSubscribableChannel> T createChannel(String channelName, Class<T> type,
															MessageHandler messageHandler)
			throws IllegalAccessException, InstantiationException {
		T channel;
		if (testBinder.getApplicationContext().containsBean(channelName)) {
			channel = testBinder.getApplicationContext().getBean(channelName, type);
		} else {
			channel = type.newInstance();
			channel.setComponentName(channelName);
			testBinder.getApplicationContext().registerBean(channelName, type, () -> channel);
		}
		channel.subscribe(messageHandler);
		return channel;
	}

	public JCSMPSession getJcsmpSession() {
		return jcsmpSession;
	}

	/**
	 * Should only be used by subclasses.
	 */
	protected void setJcsmpSession(JCSMPSession jcsmpSession) {
		this.jcsmpSession = jcsmpSession;
	}


	public <T> ConsumerInfrastructureUtil<T> createConsumerInfrastructureUtil(Class<T> type) {
		return new ConsumerInfrastructureUtil<>(this, type);
	}
}
