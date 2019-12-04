package com.solace.spring.cloud.stream.binder.config;

import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.SpringJCSMPFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import com.solace.spring.cloud.stream.binder.SolaceMessageChannelBinder;
import com.solace.spring.cloud.stream.binder.properties.SolaceExtendedBindingProperties;
import com.solace.spring.cloud.stream.binder.provisioning.SolaceQueueProvisioner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;

@Configuration
@EnableConfigurationProperties({ SolaceExtendedBindingProperties.class })
public class SolaceMessageChannelBinderConfiguration {
	@Autowired
	private SpringJCSMPFactory springJCSMPFactory;

	@Autowired
	private SolaceExtendedBindingProperties solaceExtendedBindingProperties;

	private JCSMPSession jcsmpSession;

	@PostConstruct
	private void initSession() throws JCSMPException {
		jcsmpSession = springJCSMPFactory.createSession();
		jcsmpSession.connect();
	}

	@Bean
	SolaceMessageChannelBinder solaceMessageChannelBinder() throws Exception {
		SolaceMessageChannelBinder binder = new SolaceMessageChannelBinder(jcsmpSession, provisioningProvider());
		binder.setExtendedBindingProperties(solaceExtendedBindingProperties);
		return binder;
	}

	@Bean
	SolaceQueueProvisioner provisioningProvider() {
		return new SolaceQueueProvisioner(jcsmpSession);
	}
}
