/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.solace.spring.cloud.core;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.meanbean.test.BeanTester;
import org.meanbean.test.EqualsMethodTester;

public class SolaceMessagingServiceInfoTest {

	@Test
	public void fullSetGetCredentialsTest() throws IllegalAccessException, InvocationTargetException {

		String id = "full-credentials-instance";
		String clientUsername = "sample-client-username";
		String clientPassword = "sample-client-password";
		String msgVpnName = "sample-msg-vpn";
		String smfHost = "tcp://192.168.1.50:7000";
		String smfTlsHost = "tcps://192.168.1.50:7003";
		String smfZipHost = "tcp://192.168.1.50:7001";
		String jmsJndiUri = "smf://192.168.1.50:7000";
		String jmsJndiTlsUri = "smfs://192.168.1.50:7003";
		List<String> mqttUris = Arrays.asList("tcp://192.168.1.50:7020");
		List<String> mqttTlsUris = Arrays.asList("ssl://192.168.1.50:7021", "ssl://192.168.1.51:7021");
		List<String> mqttWsUris = Arrays.asList("ws://192.168.1.50:7022");
		List<String> mqttWssUris = Arrays.asList("wss://192.168.1.50:7023", "wss://192.168.1.51:7023");
		List<String> restUris = Arrays.asList("http://192.168.1.50:7018");
		List<String> restTlsUris = Arrays.asList("https://192.168.1.50:7019");
		List<String> amqpUris = Arrays.asList("http://192.168.1.50:7016");
		List<String> amqpTlsUris = Arrays.asList("https://192.168.1.50:7017");
		List<String> managementHostnames = Arrays.asList("vmr-Medium-VMR-0");
		String managementUsername = "sample-mgmt-username";
		String managementPassword = "sample-mgmt-password";
		String activeManagementHostname = "vmr-medium-web";
		String dmrClusterName = "dmr-cluster-name";
		String dmrClusterPassword = "dmr-cluster-password";

		SolaceMessagingInfo smi = new SolaceMessagingInfo(id, clientUsername, clientPassword, msgVpnName, smfHost,
				smfTlsHost, smfZipHost, jmsJndiUri, jmsJndiTlsUri, restUris, restTlsUris, mqttUris, mqttTlsUris,
				mqttWsUris, mqttWssUris, amqpUris, amqpTlsUris, managementHostnames, managementPassword,
				managementUsername, activeManagementHostname, dmrClusterName, dmrClusterPassword);

		// Check Top Level stuff
		assertEquals(id, smi.getId());
		assertEquals(clientUsername, smi.getClientUsername());
		assertEquals(clientPassword, smi.getClientPassword());
		assertEquals(msgVpnName, smi.getMsgVpnName());

		// Check SMF
		assertEquals("tcp://192.168.1.50:7000", smi.getSmfHost());
		assertEquals("tcps://192.168.1.50:7003", smi.getSmfTlsHost());
		assertEquals("tcp://192.168.1.50:7001", smi.getSmfZipHost());

		// Check JMS
		assertEquals("smf://192.168.1.50:7000", smi.getJmsJndiUri());
		assertEquals("smfs://192.168.1.50:7003", smi.getJmsJndiTlsUri());

		// Check MQTT
		assertThat(smi.getMqttUris(), is(Arrays.asList("tcp://192.168.1.50:7020")));
		assertThat(smi.getMqttTlsUris(), is(Arrays.asList("ssl://192.168.1.50:7021", "ssl://192.168.1.51:7021")));
		assertThat(smi.getMqttWsUris(), is(Arrays.asList("ws://192.168.1.50:7022")));
		assertThat(smi.getMqttWssUris(), is(Arrays.asList("wss://192.168.1.50:7023", "wss://192.168.1.51:7023")));

		// Check REST
		assertThat(smi.getRestUris(), is(restUris));
		assertThat(smi.getRestTlsUris(), is(restTlsUris));

		// Check AMQP
		assertThat(smi.getAmqpUris(), is(amqpUris));
		assertThat(smi.getAmqpTlsUris(), is(amqpTlsUris));

		// Check Management Interfaces
		assertThat(smi.getManagementHostnames(), is(managementHostnames));
		assertEquals(managementUsername, smi.getManagementUsername());
		assertEquals(managementPassword, smi.getManagementPassword());

		// Check DMR Cluster Credentials
		assertEquals(dmrClusterName, smi.getDmrClusterName());
		assertEquals(dmrClusterPassword, smi.getDmrClusterPassword());
	}

	@Test
	public void meanBeanGetterAndSetterCorrectness() throws Exception {
		new BeanTester().testBean(SolaceMessagingInfo.class);
	}

	@Test
	public void meanBeanEqualsAndHashCodeContract() {
		EqualsMethodTester tester = new EqualsMethodTester();
		tester.testEqualsMethod(SolaceMessagingInfo.class);
	}
}
