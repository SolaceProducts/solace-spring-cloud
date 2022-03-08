package com.solace.spring.cloud.stream.binder.test.spring;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.DefaultPollableMessageSource;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PartitionCapableBinderTests;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.binder.Spy;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.AbstractSubscribableChannel;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

public class SpringCloudStreamContext extends PartitionCapableBinderTests<SolaceTestBinder,
		ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>>
		implements ExtensionContext.Store.CloseableResource {
	private static final Logger LOGGER = LoggerFactory.getLogger(SpringCloudStreamContext.class);
	private JCSMPSession jcsmpSession;
	private SempV2Api sempV2Api;

	public SpringCloudStreamContext(JCSMPSession jcsmpSession, SempV2Api sempV2Api) {
		this.jcsmpSession = Objects.requireNonNull(jcsmpSession);
		this.sempV2Api = sempV2Api;
	}

	/**
	 * Should only be used by subclasses.
	 */
	protected SpringCloudStreamContext() {
		this.jcsmpSession = null;
		this.sempV2Api = null;
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
			testBinder = new SolaceTestBinder(jcsmpSession, sempV2Api);
		}
		return testBinder;
	}

	@Override
	public ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
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

	/**
	 * Should only be used by subclasses.
	 */
	protected void setSempV2Api(SempV2Api sempV2Api) {
		this.sempV2Api = sempV2Api;
	}

	public <T> ConsumerInfrastructureUtil<T> createConsumerInfrastructureUtil(Class<T> type) {
		return new ConsumerInfrastructureUtil<>(type);
	}

	/**
	 * Utility class that abstracts away the differences between asynchronous and polled consumer-related operations.
	 * @param <T> The channel type
	 */
	public class ConsumerInfrastructureUtil<T> {
		private final Class<T> type;

		private ConsumerInfrastructureUtil(Class<T> type) {
			this.type = type;
		}

		public T createChannel(String channelName, BindingProperties bindingProperties) throws Exception {
			if (type.equals(DirectChannel.class)) {
				@SuppressWarnings("unchecked")
				T channel = (T) createBindableChannel(channelName, bindingProperties);
				return channel;
			} else if (type.equals(PollableSource.class)) {
				@SuppressWarnings("unchecked")
				T channel = (T) createBindableMessageSource(channelName, bindingProperties);
				return channel;
			} else {
				throw new UnsupportedOperationException("type not supported: " + type);
			}
		}

		public Binding<T> createBinding(SolaceTestBinder binder, String destination, String group, T channel,
										ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties) {
			if (type.equals(DirectChannel.class)) {
				@SuppressWarnings("unchecked")
				Binding<T> binding = (Binding<T>) binder.bindConsumer(destination, group, (DirectChannel) channel,
						consumerProperties);
				return binding;
			} else if (type.equals(PollableSource.class)) {
				@SuppressWarnings("unchecked")
				Binding<T> binding = (Binding<T>) binder.bindPollableConsumer(destination, group,
						(PollableSource<MessageHandler>) channel, consumerProperties);
				return binding;
			} else {
				throw new UnsupportedOperationException("type not supported: " + type);
			}
		}

		public void sendAndSubscribe(T inputChannel, DirectChannel outputChannel, Message<?> message,
									 Consumer<Message<?>> fnc) throws InterruptedException {
			sendAndSubscribe(inputChannel, outputChannel, message, fnc, 1);
		}

		public void sendAndSubscribe(T inputChannel, DirectChannel outputChannel, Message<?> message,
									 Consumer<Message<?>> fnc, int numMessagesToReceive) throws InterruptedException {
			sendAndSubscribe(inputChannel, outputChannel, message, (msg, callback) -> {
				fnc.accept(msg);
				callback.run();
			}, numMessagesToReceive);
		}

		public void sendAndSubscribe(T inputChannel, DirectChannel outputChannel, Message<?> message,
									 BiConsumer<Message<?>, Runnable> fnc) throws InterruptedException {
			sendAndSubscribe(inputChannel, outputChannel, message, fnc, 1);
		}

		public void sendAndSubscribe(T inputChannel, DirectChannel outputChannel, Message<?> message,
									 BiConsumer<Message<?>, Runnable> fnc, int numMessagesToReceive)
				throws InterruptedException {
			if (type.equals(DirectChannel.class)) {
				final CountDownLatch latch = new CountDownLatch(numMessagesToReceive);
				((DirectChannel) inputChannel).subscribe(msg -> fnc.accept(msg, latch::countDown));

				outputChannel.send(message);
				assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			} else if (type.equals(PollableSource.class)) {
				outputChannel.send(message);

				@SuppressWarnings("unchecked")
				PollableSource<MessageHandler> pollableSource = (PollableSource<MessageHandler>) inputChannel;
				final CountDownLatch latch = new CountDownLatch(numMessagesToReceive);
				for (int i = 0; latch.getCount() > 0 && i < 100; i ++) {
					boolean gotMessage = false;
					for (int j = 0; latch.getCount() > 0 && !gotMessage && j < 100; j++) {
						LOGGER.info(String.format("Poll: %s, latch count: %s", (i * 100) + j, latch.getCount()));
						try {
							gotMessage = pollableSource.poll(msg -> fnc.accept(msg, latch::countDown));
						} catch (MessagingException e) {
							if (e.getCause() instanceof JCSMPTransportException &&
									e.getCause().getMessage().contains("was closed while in receive")) {
								LOGGER.info(String.format("Absorbing %s", JCSMPTransportException.class));
							} else {
								throw e;
							}
						}
					}
				}
				assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
			} else {
				throw new UnsupportedOperationException("type not supported: " + type);
			}
		}
	}
}
