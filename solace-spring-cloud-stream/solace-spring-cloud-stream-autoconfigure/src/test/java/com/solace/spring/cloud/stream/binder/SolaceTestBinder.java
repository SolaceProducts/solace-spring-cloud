package com.solace.spring.cloud.stream.binder;

import com.solace.spring.cloud.stream.binder.properties.SolaceConsumerProperties;
import com.solace.spring.cloud.stream.binder.properties.SolaceProducerProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import com.solace.spring.cloud.stream.binder.util.SolaceProvisioningUtil;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.Queue;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.stream.binder.AbstractPollableConsumerTestBinder;
import org.springframework.cloud.stream.binder.Binding;
import org.springframework.cloud.stream.binder.ExtendedConsumerProperties;
import org.springframework.cloud.stream.binder.ExtendedProducerProperties;
import org.springframework.cloud.stream.binder.PollableSource;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class SolaceTestBinder
		extends AbstractPollableConsumerTestBinder<SolaceMessageChannelBinder, ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>> {

	private final JCSMPSession jcsmpSession;
	private final AnnotationConfigApplicationContext applicationContext;
	private final Set<String> queues = new HashSet<>();
	private static final Log logger = LogFactory.getLog(SolaceTestBinder.class);

	public SolaceTestBinder(JCSMPSession jcsmpSession) throws Exception {
		this.applicationContext = new AnnotationConfigApplicationContext(Config.class);
		this.jcsmpSession = jcsmpSession;
		jcsmpSession.connect();
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, new SolaceQueueProvisioner(jcsmpSession));
		binder.setApplicationContext(this.applicationContext);
		this.setPollableConsumerBinder(binder);
	}

	public AnnotationConfigApplicationContext getApplicationContext() {
		return applicationContext;
	}

	@Override
	public Binding<MessageChannel> bindConsumer(String name, String group, MessageChannel moduleInputChannel,
												ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		captureConsumerResources(name, group, properties.getExtension());
		return super.bindConsumer(name, group, moduleInputChannel, properties);
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name, String group,
																		PollableSource<MessageHandler> inboundBindTarget,
																		ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		captureConsumerResources(name, group, properties.getExtension());
		return super.bindPollableConsumer(name, group, inboundBindTarget, properties);
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel,
												ExtendedProducerProperties<SolaceProducerProperties> properties) {
		if (properties.getRequiredGroups() != null) {
			Arrays.stream(properties.getRequiredGroups())
					.forEach(g -> captureProducerResources(name, g, properties.getExtension()));
		}

		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	private void captureConsumerResources(String name, String group, SolaceConsumerProperties consumerProperties) {
		boolean isAnonQueue = SolaceProvisioningUtil.isAnonQueue(group);
		String queueName = SolaceProvisioningUtil.getQueueName(name, group, consumerProperties, isAnonQueue);

		if (!isAnonQueue) {
			queues.add(queueName);
		}

		if (consumerProperties.isAutoBindDmq()) {
			queues.add(SolaceProvisioningUtil.getDMQName(queueName));
		}
	}

	private void captureProducerResources(String name, String group, SolaceProducerProperties producerProperties) {
		String queueName = SolaceProvisioningUtil.getQueueName(name, group, producerProperties);
		queues.add(queueName);
	}

	@Override
	public void cleanup() {
		for (String queueName : queues) {
			try {
				logger.info(String.format("De-provisioning queue %s", queueName));
				Queue queue = JCSMPFactory.onlyInstance().createQueue(queueName);
				jcsmpSession.deprovision(queue, JCSMPSession.FLAG_IGNORE_DOES_NOT_EXIST);
			} catch (JCSMPException e) {
				throw new RuntimeException(e);
			}
		}
		jcsmpSession.closeSession();
	}

	@Configuration
	@EnableIntegration
	static class Config {}
}
