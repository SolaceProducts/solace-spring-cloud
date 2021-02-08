package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.test.util.IgnoreInheritedTests;
import com.solace.spring.cloud.stream.binder.test.util.InheritedTestsFilteredRunner;
import com.solace.spring.cloud.stream.binder.test.util.SolaceExternalResourceHandler;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solace.test.integration.semp.v2.SempV2Api;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPTransportException;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.stream.binder.Binding;
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
import org.springframework.messaging.SubscribableChannel;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * <p>Base class for all Solace Spring Cloud Stream Binder test classes.
 *
 * <p>Typically, you'll want to filter out all inherited test cases from
 * the parent class {@link PartitionCapableBinderTests PartitionCapableBinderTests}.
 * To do this, run your test class with the
 * {@link InheritedTestsFilteredRunner InheritedTestsFilteredSpringRunner} runner
 * along with the {@link IgnoreInheritedTests @IgnoreInheritedTests} annotation.
 */
public abstract class SolaceBinderITBase
		extends PartitionCapableBinderTests<SolaceTestBinder, ExtendedConsumerProperties<SolaceConsumerProperties>,
		ExtendedProducerProperties<SolaceProducerProperties>> {
	@ClassRule
	public static final SpringClassRule springClassRule = new SpringClassRule();

	@Rule
	public final SpringMethodRule springMethodRule = new SpringMethodRule();

	@Autowired
	private SpringJCSMPFactory springJCSMPFactory;

	@Value("${test.solace.mgmt.host:#{null}}")
	private String solaceMgmtHost;

	@Value("${test.solace.mgmt.username:#{null}}")
	private String solaceMgmtUsername;

	@Value("${test.solace.mgmt.password:#{null}}")
	private String solaceMgmtPassword;

	JCSMPSession jcsmpSession;
	SempV2Api sempV2Api;
	String msgVpnName;

	static SolaceExternalResourceHandler externalResource = new SolaceExternalResourceHandler();

	@Before
	public void setupSempV2Api() {
		assertThat(solaceMgmtHost).as("test.solace.mgmt.host cannot be blank").isNotBlank();
		assertThat(solaceMgmtUsername).as("test.solace.mgmt.username cannot be blank").isNotBlank();
		assertThat(solaceMgmtPassword).as("test.solace.mgmt.password cannot be blank").isNotBlank();
		sempV2Api = new SempV2Api(solaceMgmtHost, solaceMgmtUsername, solaceMgmtPassword);
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
	protected SolaceTestBinder getBinder() throws Exception {
		if (testBinder == null || jcsmpSession.isClosed()) {
			if (testBinder != null) {
				logger.info(String.format("Will recreate %s since %s is closed",
						testBinder.getClass().getSimpleName(), jcsmpSession.getClass().getSimpleName()));
				testBinder.getBinder().destroy();
				testBinder = null;
			}

			logger.info(String.format("Getting new %s instance", SolaceTestBinder.class.getSimpleName()));
			jcsmpSession = externalResource.getActiveSession(springJCSMPFactory);
			msgVpnName = (String) jcsmpSession.getProperty(JCSMPProperties.VPN_NAME);
			testBinder = new SolaceTestBinder(jcsmpSession);
		}
		return testBinder;
	}

	@Override
	protected ExtendedConsumerProperties<SolaceConsumerProperties> createConsumerProperties() {
		return new ExtendedConsumerProperties<>(new SolaceConsumerProperties());
	}

	@Override
	protected ExtendedProducerProperties<SolaceProducerProperties> createProducerProperties() {
		return new ExtendedProducerProperties<>(new SolaceProducerProperties());
	}

	@Override
	public Spy spyOn(String name) {
		return null;
	}

	<T extends AbstractSubscribableChannel> T createChannel(String channelName, Class<T> type,
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

	/**
	 * Utility class that abstracts away the differences between asynchronous and polled consumer-related operations.
	 * @param <T> The channel type
	 */
	class ConsumerInfrastructureUtil<T> {
		private final Class<T> type;

		public ConsumerInfrastructureUtil(Class<T> type) {
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
						logger.info(String.format("Poll: %s, latch count: %s", (i * 100) + j, latch.getCount()));
						try {
							gotMessage = pollableSource.poll(msg -> fnc.accept(msg, latch::countDown));
						} catch (MessagingException e) {
							if (e.getCause() instanceof JCSMPTransportException &&
									e.getCause().getMessage().contains("was closed while in receive")) {
								logger.info(String.format("Absorbing %s", JCSMPTransportException.class));
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
