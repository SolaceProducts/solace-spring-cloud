package com.solace.spring.cloud.stream.binder.test.spring;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.test.util.SolaceTestBinder;
import com.solacesystems.jcsmp.JCSMPTransportException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.cloud.stream.config.BindingProperties;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.MessagingException;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static com.solace.spring.cloud.stream.binder.test.util.RetryableAssertions.retryAssert;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Utility class that abstracts away the differences between asynchronous and polled consumer-related operations.
 *
 * @param <T> The channel type
 */
public class ConsumerInfrastructureUtil<T> {
    private final SpringCloudStreamContext context;
    private final Class<T> type;

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerInfrastructureUtil.class);

    ConsumerInfrastructureUtil(SpringCloudStreamContext context, Class<T> type) {
        this.context = context;
        this.type = type;
    }

    public T createChannel(String channelName, BindingProperties bindingProperties) throws Exception {
        if (type.equals(DirectChannel.class)) {
            @SuppressWarnings("unchecked")
            T channel = (T) context.createBindableChannel(channelName, bindingProperties);
            return channel;
        } else if (type.equals(PollableSource.class)) {
            @SuppressWarnings("unchecked")
            T channel = (T) context.createBindableMessageSource(channelName, bindingProperties);
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

    /**
     * Async message subscribe.
     *
     * @param inputChannel    input channel
     * @param executorService executor service
     * @param messageHandler  message handler
     */
    public void subscribe(T inputChannel, ScheduledExecutorService executorService,
                          Consumer<Message<?>> messageHandler) {
        if (type.equals(DirectChannel.class)) {
            ((DirectChannel) inputChannel).subscribe(messageHandler::accept);
        } else if (type.equals(PollableSource.class)) {
            @SuppressWarnings("unchecked")
            PollableSource<MessageHandler> pollableSource = (PollableSource<MessageHandler>) inputChannel;
            executorService.scheduleAtFixedRate(() -> {
                try {
                    if (!pollableSource.poll(messageHandler::accept)) {
                        Thread.sleep(1000);
                    }
                } catch (Throwable t) {
                    LOGGER.error("exception received while polling for messages", t);
                }
            }, 0, 1, TimeUnit.MILLISECONDS);
        } else {
            throw new UnsupportedOperationException("type not supported: " + type);
        }
    }

    public void sendAndSubscribe(T inputChannel, int numMessagesToReceive, Runnable sendMessagesFn,
                                 Consumer<Message<?>> messageHandler) throws InterruptedException {
        sendAndSubscribe(inputChannel, numMessagesToReceive, sendMessagesFn, (msg, callback) -> {
            messageHandler.accept(msg);
            callback.run();
        });
    }

    public void sendAndSubscribe(T inputChannel, int numMessagesToReceive, Runnable sendMessagesFn,
                                 BiConsumer<Message<?>, Runnable> messageHandler)
            throws InterruptedException {
        if (type.equals(DirectChannel.class)) {
            final CountDownLatch latch = new CountDownLatch(numMessagesToReceive);
            MessageHandler handler = msg -> messageHandler.accept(msg, latch::countDown);
            ((DirectChannel) inputChannel).subscribe(handler);

            if (sendMessagesFn != null) {
                sendMessagesFn.run();
            }
            assertThat(latch.await(3, TimeUnit.MINUTES)).isTrue();
            ((DirectChannel) inputChannel).unsubscribe(handler);
        } else if (type.equals(PollableSource.class)) {
            if (sendMessagesFn != null) {
                sendMessagesFn.run();
            }

            @SuppressWarnings("unchecked")
            PollableSource<MessageHandler> pollableSource = (PollableSource<MessageHandler>) inputChannel;
            final CountDownLatch latch = new CountDownLatch(numMessagesToReceive);
            // PollableSource.poll(...) only returns true for successful message handling.
            // Meaning: latch.getCount() < actual-#-of-iterations < numMessagesToReceive
            for (int i = 0; latch.getCount() > 0 && i < numMessagesToReceive; i++) {
                LOGGER.info("Polling for messages. {} remaining", latch.getCount());
                retryAssert(() -> {
                    try {
                        assertThat(pollableSource.poll(msg -> messageHandler.accept(msg, latch::countDown)))
                                .isTrue();
                    } catch (MessagingException e) {
                        if (e.getCause() instanceof JCSMPTransportException &&
                                e.getCause().getMessage().contains("was closed while in receive")) {
                            LOGGER.info(String.format("Absorbing %s", JCSMPTransportException.class));
                        } else if (ExceptionUtils.getRootCause(e) instanceof ExpectedMessageHandlerException) {
                            LOGGER.info("Received expected exception", ExceptionUtils.getRootCause(e));
                        } else {
                            throw e;
                        }
                    }
                });
            }
            assertThat(latch.await(3, TimeUnit.MINUTES)).isTrue();
        } else {
            throw new UnsupportedOperationException("type not supported: " + type);
        }
    }

    public static class ExpectedMessageHandlerException extends RuntimeException {
        public ExpectedMessageHandlerException(String message) {
            super(message);
        }
    }
}
