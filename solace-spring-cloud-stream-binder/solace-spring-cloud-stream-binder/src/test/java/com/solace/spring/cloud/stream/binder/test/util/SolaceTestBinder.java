package com.solace.spring.cloud.stream.binder.test.util;

import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
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
import org.springframework.util.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;

public class SolaceTestBinder
		extends AbstractPollableConsumerTestBinder<SolaceMessageChannelBinder, ExtendedConsumerProperties<SolaceConsumerProperties>, ExtendedProducerProperties<SolaceProducerProperties>> {

	private final JCSMPSession jcsmpSession;
	private final AnnotationConfigApplicationContext applicationContext;
	private final Set<String> queues = new HashSet<>();
	private final Map<String, String> bindingNameToQueueName = new HashMap<>();
	private final Map<String, String> bindingNameToErrorQueueName = new HashMap<>();
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
		preBindCaptureConsumerResources(name, group, properties.getExtension());
		Binding<MessageChannel> binding = super.bindConsumer(name, group, moduleInputChannel, properties);
		captureConsumerResources(binding, name, group, properties.getExtension());
		return binding;
	}

	@Override
	public Binding<PollableSource<MessageHandler>> bindPollableConsumer(String name, String group,
																		PollableSource<MessageHandler> inboundBindTarget,
																		ExtendedConsumerProperties<SolaceConsumerProperties> properties) {
		preBindCaptureConsumerResources(name, group, properties.getExtension());
		Binding<PollableSource<MessageHandler>> binding = super.bindPollableConsumer(name, group, inboundBindTarget, properties);
		captureConsumerResources(binding, name, group, properties.getExtension());
		return binding;
	}

	@Override
	public Binding<MessageChannel> bindProducer(String name, MessageChannel moduleOutputChannel,
												ExtendedProducerProperties<SolaceProducerProperties> properties) {
		if (properties.getRequiredGroups() != null) {
			Arrays.stream(properties.getRequiredGroups())
					.forEach(g -> preBindCaptureProducerResources(name, g, properties.getExtension()));
		}

		return super.bindProducer(name, moduleOutputChannel, properties);
	}

	public String getConsumerQueueName(Binding<?> binding) {
		return bindingNameToQueueName.get(binding.getBindingName());
	}

	public String getConsumerErrorQueueName(Binding<?> binding) {
		return bindingNameToErrorQueueName.get(binding.getBindingName());
	}

	private void preBindCaptureConsumerResources(String name, String group, SolaceConsumerProperties consumerProperties) {
		if (SolaceProvisioningUtil.isAnonQueue(group)) return; // we don't know any anon resource names before binding

		SolaceProvisioningUtil.QueueNames queueNames = SolaceProvisioningUtil.getQueueNames(name, group,
				consumerProperties, false);

		// values set here may be overwritten after binding
		queues.add(queueNames.getConsumerGroupQueueName());
		if (consumerProperties.isAutoBindErrorQueue()) {
			queues.add(queueNames.getErrorQueueName());
		}
	}

	private void captureConsumerResources(Binding<?> binding, String name, String group, SolaceConsumerProperties consumerProperties) {
		String queueName = extractBindingDestination(binding);
		bindingNameToQueueName.put(binding.getBindingName(), queueName);
		if (!SolaceProvisioningUtil.isAnonQueue(group)) {
			queues.add(queueName);
		}
		if (consumerProperties.isAutoBindErrorQueue()) {
			String errorQueueName = StringUtils.hasText(consumerProperties.getErrorQueueNameOverride()) ?
					consumerProperties.getErrorQueueNameOverride() :
					extractErrorQueueName(binding, name, group, consumerProperties.isUseGroupNameInErrorQueueName());
			queues.add(errorQueueName);
			bindingNameToErrorQueueName.put(binding.getBindingName(), errorQueueName);
		}
	}

	private void preBindCaptureProducerResources(String name, String group, SolaceProducerProperties producerProperties) {
		String queueName = SolaceProvisioningUtil.getQueueName(name, group, producerProperties);
		queues.add(queueName);
	}

	private String extractBindingDestination(Binding<?> binding) {
		String destination = (String) binding.getExtendedInfo().getOrDefault("bindingDestination", "");
		assertThat(destination).startsWith("SolaceConsumerDestination");
		Matcher matcher = Pattern.compile("queueName='(.*?)'").matcher(destination);
		assertThat(matcher.find()).isTrue();
		return matcher.group(1);
	}

	private String extractErrorQueueName(Binding<?> binding, String destination, String group, boolean includeGroup) {
		String fullQueueName = extractBindingDestination(binding);
		String prefix;
		if (fullQueueName.startsWith("#P2P/QTMP/")) {
			prefix = fullQueueName.substring(fullQueueName.indexOf(destination));
		} else if (includeGroup && !fullQueueName.endsWith('.' + group)) {
			prefix = fullQueueName + '.' + group;
		} else if (!includeGroup && fullQueueName.endsWith('.' + group)) {
			prefix = fullQueueName.replace('.' + group, "");
		} else {
			prefix = fullQueueName;
		}
		return prefix + ".error";
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
