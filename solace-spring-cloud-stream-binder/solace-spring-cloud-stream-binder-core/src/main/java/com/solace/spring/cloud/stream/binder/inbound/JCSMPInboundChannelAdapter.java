package com.solace.spring.cloud.stream.binder.inbound;

import com.solace.spring.cloud.stream.binder.health.SolaceBinderHealthAccessor;
import com.solace.spring.cloud.stream.binder.inbound.acknowledge.JCSMPAcknowledgementCallbackFactory;
import com.solace.spring.cloud.stream.binder.meter.SolaceMeterAccessor;
import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.EndpointProvider;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceConsumerDestination;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceProvisioningUtil;
import com.solace.spring.cloud.stream.binder.util.EndpointType;
import com.solace.spring.cloud.stream.binder.util.ErrorQueueInfrastructure;
import com.solace.spring.cloud.stream.binder.util.FlowReceiverContainer;
import com.solace.spring.cloud.stream.binder.util.SolaceMessageConversionException;
import com.solacesystems.jcsmp.ConsumerFlowProperties;
import com.solacesystems.jcsmp.Endpoint;
import com.solacesystems.jcsmp.EndpointProperties;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.XMLMessage.Outcome;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import com.solacesystems.jcsmp.transaction.RollbackException;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.core.AttributeAccessor;
import org.springframework.integration.context.OrderlyShutdownCapable;
import org.springframework.integration.core.Pausable;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.lang.Nullable;
import org.springframework.messaging.MessagingException;
import org.springframework.retry.RecoveryCallback;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.scheduling.concurrent.CustomizableThreadFactory;
import org.springframework.util.Assert;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class JCSMPInboundChannelAdapter extends MessageProducerSupport implements OrderlyShutdownCapable, Pausable {
    private final String id = UUID.randomUUID().toString();
    private final SolaceConsumerDestination consumerDestination;
    private final JCSMPSession jcsmpSession;
    private final ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties;
    private final EndpointProperties endpointProperties;
    @Nullable
    private final SolaceMeterAccessor solaceMeterAccessor;
    private final long shutdownInterruptThresholdInMillis = 500; //TODO Make this configurable
    private final List<FlowReceiverContainer> flowReceivers;
    private final Set<AtomicBoolean> consumerStopFlags;
    private final AtomicBoolean paused = new AtomicBoolean(false);
    private Consumer<Endpoint> postStart;
    private ExecutorService executorService;
    private AtomicBoolean remoteStopFlag;
    private RetryTemplate retryTemplate;
    private RecoveryCallback<?> recoveryCallback;
    private ErrorQueueInfrastructure errorQueueInfrastructure;
    @Nullable
    private SolaceBinderHealthAccessor solaceBinderHealthAccessor;

    // overriding Spring's LogAccessor to just use plain SLF4J
    private static final Logger logger = LoggerFactory.getLogger(JCSMPInboundChannelAdapter.class);
    private static final ThreadLocal<AttributeAccessor> attributesHolder = new ThreadLocal<>();

    public JCSMPInboundChannelAdapter(SolaceConsumerDestination consumerDestination,
                                      JCSMPSession jcsmpSession,
                                      ExtendedConsumerProperties<SolaceConsumerProperties> consumerProperties,
                                      @Nullable EndpointProperties endpointProperties,
                                      @Nullable SolaceMeterAccessor solaceMeterAccessor) {
        this.consumerDestination = consumerDestination;
        this.jcsmpSession = jcsmpSession;
        this.consumerProperties = consumerProperties;
        this.endpointProperties = endpointProperties;
        this.solaceMeterAccessor = solaceMeterAccessor;
        this.flowReceivers = new ArrayList<>(consumerProperties.getConcurrency());
        this.consumerStopFlags = new HashSet<>(consumerProperties.getConcurrency());
    }

    @Override
    protected void doStart() {
        final String endpointName = consumerDestination.getName();
		logger.info("Creating {} consumer flows for {} {} <inbound adapter {}>",
                consumerProperties.getExtension().getEndpointType(), consumerProperties.getConcurrency(), endpointName, id);

        if (isRunning()) {
			logger.warn("Nothing to do. Inbound message channel adapter {} is already running", id);
            return;
        }

        if (consumerProperties.getConcurrency() < 1) {
            MessagingException exception = new MessagingException(
                    String.format("Concurrency must be greater than 0, was %s <inbound adapter %s>",
                    consumerProperties.getConcurrency(), id));
            logger.warn(exception.getMessage());
            throw exception;
        }

        Map<String, String> headerNameMapping = consumerProperties.getExtension().getHeaderNameMapping();
        if (headerNameMapping != null && !headerNameMapping.isEmpty()) {
            Set<String> targetHeaderNames = new HashSet<>(headerNameMapping.values());
            if (targetHeaderNames.size() < headerNameMapping.size()) {
                MessagingException exception = new MessagingException(String.format(
                    "Two or more keys map to the same header name in headerNameMapping %s <inbound adapter %s>",
                    consumerProperties.getExtension().getHeaderNameMapping(), id));
                logger.warn(exception.getMessage());
                throw exception;
            }
        }

        if (jcsmpSession instanceof JCSMPBasicSession jcsmpBasicSession
                && !jcsmpBasicSession.isRequiredSettlementCapable(
                Set.of(Outcome.ACCEPTED, Outcome.FAILED, Outcome.REJECTED))) {
            throw new MessagingException(String.format(
                    "The Solace PubSub+ Broker doesn't support message NACK capability, <inbound adapter %s>", id));
        }

        if (executorService != null && !executorService.isTerminated()) {
			logger.warn("Unexpectedly found running executor service while starting inbound adapter {}, closing it...", id);
            stopAllConsumers();
        }

        EndpointType endpointType = consumerProperties.getExtension().getEndpointType();
        EndpointProvider<?> endpointProvider = EndpointProvider.from(endpointType);

        if (endpointProvider == null) {
            MessagingException exception = new MessagingException(String.format(
                    "Consumer not supported for destination type %s <inbound adapter %s>",
                    consumerProperties.getExtension().getEndpointType(), id));
            logger.warn(exception.getMessage());
            throw exception;
        }

        Endpoint endpoint = null;
        try {
            // JCSMP applies naming rules for queues not for topic endpoints.
            // this code is to prevent queue endpoints get bad names and temp topic endpoints stay temporary
            endpoint = (EndpointType.TOPIC_ENDPOINT.equals(endpointType) && consumerDestination.isTemporary()) ?
                    endpointProvider.createTemporaryEndpoint(endpointName, jcsmpSession)
                    : endpointProvider.createInstance(endpointName);
        } catch (JCSMPException e) {
            // consumer binding will fail later, just logging
			logger.warn("Inbound adapter {} can't create temporary endpoint {} on a broker", id, endpointName, e);
        }

        ConsumerFlowProperties consumerFlowProperties = SolaceProvisioningUtil.getConsumerFlowProperties(
                consumerDestination.getBindingDestinationName(), consumerProperties);

        for (int i = 0, numToCreate = consumerProperties.getConcurrency() - flowReceivers.size(); i < numToCreate; i++) {
			logger.info("Creating consumer {} of {} for inbound adapter {}", i + 1, consumerProperties.getConcurrency(), id);

            FlowReceiverContainer flowReceiverContainer = new FlowReceiverContainer(
                    jcsmpSession,
                    endpoint,
                    consumerProperties.getExtension().isTransacted(),
                    endpointProperties,
                    consumerFlowProperties);

            if (paused.get()) {
				logger.info("Inbound adapter {} is paused, pausing newly created flow receiver container {}", id,
                        flowReceiverContainer.getId());
                flowReceiverContainer.pause();
            }
            flowReceivers.add(flowReceiverContainer);
        }

        if (solaceBinderHealthAccessor != null) {
            for (int i = 0; i < flowReceivers.size(); i++) {
                solaceBinderHealthAccessor.addFlow(consumerProperties.getBindingName(), i, flowReceivers.get(i));
            }
        }

        try {
            for (FlowReceiverContainer flowReceiverContainer : flowReceivers) {
                flowReceiverContainer.bind();
            }
        } catch (JCSMPException e) {
            MessagingException wrappedException = new MessagingException(String.format("Failed to get message consumer for inbound adapter %s", id), e);
            logger.warn(wrappedException.getMessage(), e);
            flowReceivers.forEach(FlowReceiverContainer::unbind);
            throw wrappedException;
        }

        if (retryTemplate != null) {
            retryTemplate.registerListener(new SolaceRetryListener(endpointName));
        }

        executorService = buildThreadPool(consumerProperties.getConcurrency(), consumerProperties.getBindingName());
        flowReceivers.stream()
                .map(this::buildListener)
                .forEach(listener -> {
                    consumerStopFlags.add(listener.getStopFlag());
                    executorService.submit(listener);
                });
        executorService.shutdown(); // All tasks have been submitted

        if (postStart != null) {
            postStart.accept(endpoint);
        }
    }

    private ExecutorService buildThreadPool(int concurrency, String bindingName) {
        ThreadFactory threadFactory = new CustomizableThreadFactory("solace-scst-consumer-" + bindingName);
        return Executors.newFixedThreadPool(concurrency, threadFactory);
    }

    @Override
    protected void doStop() {
        if (!isRunning()) return;
        stopAllConsumers();
    }

    private void stopAllConsumers() {
        final String queueName = consumerDestination.getName();
		logger.info("Stopping all {} consumer flows to queue {} <inbound adapter ID: {}>",
                consumerProperties.getConcurrency(), queueName, id);
        consumerStopFlags.forEach(flag -> flag.set(true)); // Mark threads for shutdown
        try {
            if (!executorService.awaitTermination(shutdownInterruptThresholdInMillis, TimeUnit.MILLISECONDS)) {
                logger.info("Interrupting all workers for inbound adapter {}", id);
                executorService.shutdownNow();
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    MessagingException exception = new MessagingException(String.format("executor service shutdown for inbound adapter %s timed out", id));
                    logger.warn(exception.getMessage());
                    throw exception;
                }
            }

            if (solaceBinderHealthAccessor != null) {
                for (int i = 0; i < flowReceivers.size(); i++) {
                    solaceBinderHealthAccessor.removeFlow(consumerProperties.getBindingName(), i);
                }
            }

            // cleanup
            consumerStopFlags.clear();

        } catch (InterruptedException e) {
            MessagingException wrappedException = new MessagingException(
                    String.format("executor service shutdown for inbound adapter %s was interrupted", id));
            logger.warn(wrappedException.getMessage());
            throw wrappedException;
        }
    }

    @Override
    public int beforeShutdown() {
        this.stop();
        return 0;
    }

    @Override
    public int afterShutdown() {
        return 0;
    }

    public void setPostStart(Consumer<Endpoint> postStart) {
        this.postStart = postStart;
    }

    public void setRetryTemplate(RetryTemplate retryTemplate) {
        this.retryTemplate = retryTemplate;
    }

    public void setRecoveryCallback(RecoveryCallback<?> recoveryCallback) {
        this.recoveryCallback = recoveryCallback;
    }

    public void setErrorQueueInfrastructure(ErrorQueueInfrastructure errorQueueInfrastructure) {
        this.errorQueueInfrastructure = errorQueueInfrastructure;
    }

    public void setRemoteStopFlag(AtomicBoolean remoteStopFlag) {
        this.remoteStopFlag = remoteStopFlag;
    }

    public void setSolaceBinderHealthAccessor(@Nullable SolaceBinderHealthAccessor solaceBinderHealthAccessor) {
        this.solaceBinderHealthAccessor = solaceBinderHealthAccessor;
    }

    @Override
    protected AttributeAccessor getErrorMessageAttributes(org.springframework.messaging.Message<?> message) {
        AttributeAccessor attributes = attributesHolder.get();
        return attributes == null ? super.getErrorMessageAttributes(message) : attributes;
    }

    private InboundXMLMessageListener buildListener(FlowReceiverContainer flowReceiverContainer) {
        JCSMPAcknowledgementCallbackFactory ackCallbackFactory = new JCSMPAcknowledgementCallbackFactory(
                flowReceiverContainer);
        ackCallbackFactory.setErrorQueueInfrastructure(errorQueueInfrastructure);

        InboundXMLMessageListener listener;
        if (retryTemplate != null) {
            Assert.state(getErrorChannel() == null,
                    "Cannot have an 'errorChannel' property when a 'RetryTemplate' is provided; " +
                            "use an 'ErrorMessageSendingRecoverer' in the 'recoveryCallback' property to send " +
                            "an error message when retries are exhausted");
            listener = new RetryableInboundXMLMessageListener(
                    flowReceiverContainer,
                    consumerDestination,
                    consumerProperties,
                    consumerProperties.isBatchMode() ? new BatchCollector(consumerProperties.getExtension()) : null,
                    this::sendMessage,
                    ackCallbackFactory,
                    retryTemplate,
                    recoveryCallback,
                    solaceMeterAccessor,
                    remoteStopFlag,
                    attributesHolder
            );
        } else {
            listener = new BasicInboundXMLMessageListener(
                    flowReceiverContainer,
                    consumerDestination,
                    consumerProperties,
                    consumerProperties.isBatchMode() ? new BatchCollector(consumerProperties.getExtension()) : null,
                    this::sendMessage,
                    ackCallbackFactory,
                    this::sendErrorMessageIfNecessary,
                    solaceMeterAccessor,
                    remoteStopFlag,
                    attributesHolder,
                    this.getErrorChannel() != null
            );
        }
        return listener;
    }

    @Override
    public void pause() {
        logger.info("Pausing inbound adapter {}", id);
        flowReceivers.forEach(FlowReceiverContainer::pause);
        paused.set(true);
    }

    @Override
    public void resume() {
        logger.info("Resuming inbound adapter {}", id);
        try {
            for (FlowReceiverContainer flowReceiver : flowReceivers) {
                flowReceiver.resume();
            }
            paused.set(false);
        } catch (Exception e) {
            RuntimeException toThrow = new RuntimeException(
                    String.format("Failed to resume inbound adapter %s", id), e);
            if (paused.get()) {
                logger.error(
                        "Inbound adapter {} failed to be resumed. Resumed flow receiver containers will be re-paused",
                        id, e);
                try {
                    pause();
                } catch (Exception e1) {
                    toThrow.addSuppressed(e1);
                }
            }
            throw toThrow;
        }
    }

    @Override
    public boolean isPaused() {
        if (paused.get()) {
            for (FlowReceiverContainer flowReceiverContainer : flowReceivers) {
                if (!flowReceiverContainer.isPaused()) {
                    logger.warn("Flow receiver container {} is unexpectedly running for inbound adapter {}",
                            flowReceiverContainer.getId(), id);
                    return false;
                }
            }

            return true;
        } else {
            return false;
        }
    }

    private static final class SolaceRetryListener implements RetryListener {

        private final String queueName;

        private SolaceRetryListener(String queueName) {
            this.queueName = queueName;
        }

        @Override
        public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
            return true;
        }

        @Override
        public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {

        }

        @Override
        public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
            logger.warn("Failed to consume a message from destination {} - attempt {}", queueName, context.getRetryCount());
            for (Throwable nestedThrowable : ExceptionUtils.getThrowableList(throwable)) {
                if (nestedThrowable instanceof SolaceMessageConversionException ||
                        nestedThrowable instanceof RollbackException) {
                    // Do not retry if these exceptions are thrown
                    context.setExhaustedOnly();
                    break;
                }
            }
        }
    }
}
