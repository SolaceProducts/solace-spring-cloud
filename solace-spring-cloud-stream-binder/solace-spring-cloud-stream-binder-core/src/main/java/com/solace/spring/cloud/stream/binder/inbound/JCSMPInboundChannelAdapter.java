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
import com.solacesystems.jcsmp.*;
import com.solacesystems.jcsmp.XMLMessage.Outcome;
import com.solacesystems.jcsmp.impl.JCSMPBasicSession;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

    private static final Log logger = LogFactory.getLog(JCSMPInboundChannelAdapter.class);
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
        logger.info(String.format("Creating %s consumer flows for %s %s <inbound adapter %s>",
                consumerProperties.getExtension().getEndpointType(),
                consumerProperties.getConcurrency(), endpointName, id));

        if (isRunning()) {
            logger.warn(String.format("Nothing to do. Inbound message channel adapter %s is already running", id));
            return;
        }

        if (consumerProperties.getConcurrency() < 1) {
            String msg = String.format("Concurrency must be greater than 0, was %s <inbound adapter %s>",
                    consumerProperties.getConcurrency(), id);
            logger.warn(msg);
            throw new MessagingException(msg);
        }

        if (jcsmpSession instanceof JCSMPBasicSession jcsmpBasicSession
                && !jcsmpBasicSession.isRequiredSettlementCapable(
                Set.of(Outcome.ACCEPTED, Outcome.FAILED, Outcome.REJECTED))) {
            String msg = String.format("The Solace PubSub+ Broker doesn't support message NACK capability, <inbound adapter %s>", id);
            throw new MessagingException(msg);
        }

        if (executorService != null && !executorService.isTerminated()) {
            logger.warn(String.format("Unexpectedly found running executor service while starting inbound adapter %s, " +
                    "closing it...", id));
            stopAllConsumers();
        }

        EndpointType endpointType = consumerProperties.getExtension().getEndpointType();
        EndpointProvider<?> endpointProvider = EndpointProvider.from(endpointType);

        if (endpointProvider == null) {
            String msg = String.format("Consumer not supported for destination type %s <inbound adapter %s>",
                    consumerProperties.getExtension().getEndpointType(), id);
            logger.warn(msg);
            throw new MessagingException(msg);
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
            logger.warn(String.format("Inbound adapter %s can't create temporary endpoint %s on a broker", id), e);
        }

        ConsumerFlowProperties consumerFlowProperties = SolaceProvisioningUtil.getConsumerFlowProperties(
                consumerDestination.getBindingDestinationName(), consumerProperties);

        for (int i = 0, numToCreate = consumerProperties.getConcurrency() - flowReceivers.size(); i < numToCreate; i++) {
            logger.info(String.format("Creating consumer %s of %s for inbound adapter %s",
                    i + 1, consumerProperties.getConcurrency(), id));

            FlowReceiverContainer flowReceiverContainer = new FlowReceiverContainer(
                    jcsmpSession,
                    endpoint,
                    endpointProperties,
                    consumerFlowProperties);

            if (paused.get()) {
                logger.info(String.format(
                        "Inbound adapter %s is paused, pausing newly created flow receiver container %s",
                        id, flowReceiverContainer.getId()));
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
            String msg = String.format("Failed to get message consumer for inbound adapter %s", id);
            logger.warn(msg, e);
            flowReceivers.forEach(FlowReceiverContainer::unbind);
            throw new MessagingException(msg, e);
        }

        if (retryTemplate != null) {
            retryTemplate.registerListener(new SolaceRetryListener(endpointName));
        }

        executorService = Executors.newFixedThreadPool(consumerProperties.getConcurrency());
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

    @Override
    protected void doStop() {
        if (!isRunning()) return;
        stopAllConsumers();
    }

    private void stopAllConsumers() {
        final String queueName = consumerDestination.getName();
        logger.info(String.format("Stopping all %s consumer flows to queue %s <inbound adapter ID: %s>",
                consumerProperties.getConcurrency(), queueName, id));
        consumerStopFlags.forEach(flag -> flag.set(true)); // Mark threads for shutdown
        try {
            if (!executorService.awaitTermination(shutdownInterruptThresholdInMillis, TimeUnit.MILLISECONDS)) {
                logger.info(String.format("Interrupting all workers for inbound adapter %s", id));
                executorService.shutdownNow();
                if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                    String msg = String.format("executor service shutdown for inbound adapter %s timed out", id);
                    logger.warn(msg);
                    throw new MessagingException(msg);
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
            String msg = String.format("executor service shutdown for inbound adapter %s was interrupted", id);
            logger.warn(msg);
            throw new MessagingException(msg);
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
        logger.info(String.format("Pausing inbound adapter %s", id));
        flowReceivers.forEach(FlowReceiverContainer::pause);
        paused.set(true);
    }

    @Override
    public void resume() {
        logger.info(String.format("Resuming inbound adapter %s", id));
        try {
            for (FlowReceiverContainer flowReceiver : flowReceivers) {
                flowReceiver.resume();
            }
            paused.set(false);
        } catch (Exception e) {
            RuntimeException toThrow = new RuntimeException(
                    String.format("Failed to resume inbound adapter %s", id), e);
            if (paused.get()) {
                logger.error(String.format(
                        "Inbound adapter %s failed to be resumed. Resumed flow receiver containers will be re-paused",
                        id), e);
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
                    logger.warn(String.format(
                            "Flow receiver container %s is unexpectedly running for inbound adapter %s",
                            flowReceiverContainer.getId(), id));
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
            logger.warn(String.format("Failed to consume a message from destination %s - attempt %s",
                    queueName, context.getRetryCount()));
            for (Throwable nestedThrowable : ExceptionUtils.getThrowableList(throwable)) {
                if (nestedThrowable instanceof SolaceMessageConversionException) {
                    // Do not retry if these exceptions are thrown
                    context.setExhaustedOnly();
                    break;
                }
            }
        }
    }
}
