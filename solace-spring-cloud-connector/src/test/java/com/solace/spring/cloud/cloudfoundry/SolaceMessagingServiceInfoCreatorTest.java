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

package com.solace.spring.cloud.cloudfoundry;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.solace.spring.cloud.core.SolaceMessagingInfo;


public class SolaceMessagingServiceInfoCreatorTest {

	private String solacePubSubLabel = "solace-pubsub";

	@Test
	public void basicSolaceMessagingServiceInfoCreationTestWithSolacePubSubLabel() {

		Map<String, Object> exVcapServices = createVcapMap(solacePubSubLabel);

		SolaceMessagingInfoCreator smic = new SolaceMessagingInfoCreator();

		assertTrue(smic.accept(exVcapServices));

		SolaceMessagingInfo smi = smic.createServiceInfo(exVcapServices);

		validateExampleSmi(smi);

	}


	@Test
	public void cupsSolaceMessagingServiceInfoCreationTest() {

		Map<String, Object> exVcapServices = createCUPSVcapMap();

		SolaceMessagingInfoCreator smic = new SolaceMessagingInfoCreator();

		assertTrue(smic.accept(exVcapServices));

		SolaceMessagingInfo smi = smic.createServiceInfo(exVcapServices);

		validateExampleSmi(smi);

	}


	@Test
	public void mismatchLabelTest() {
		Map<String, Object> exVcapServices = createVcapMap();

		exVcapServices.put("label", "no-match");

		SolaceMessagingInfoCreator smic = new SolaceMessagingInfoCreator();

		assertFalse(smic.accept(exVcapServices));
	}

	// We could do a lot more negative testing. But other Spring cloud
	// connectors seem very tolerant. For now starting with this limited
	// coverage.
	@Test(expected=IllegalArgumentException.class)
	public void corruptVcapTest() {
		Map<String, Object> exVcapServices = createVcapMap();

		exVcapServices.remove("credentials");

		SolaceMessagingInfoCreator smic = new SolaceMessagingInfoCreator();

		// Should still accept it.
		assertTrue(smic.accept(exVcapServices));

		// Should be throw exception for null credentials.
		smic.createServiceInfo(exVcapServices);
	}

	@Test
	public void missingKeyValueTest() {
		// Should simply result in those values still being null.
		Map<String, Object> exVcapServices = createVcapMap();

		@SuppressWarnings("unchecked")
		Map<String, Object> exCred = (Map<String, Object>) exVcapServices.get("credentials");
		exCred.remove("smfHosts");

		SolaceMessagingInfoCreator smic = new SolaceMessagingInfoCreator();

		// Should still accept it.
		assertTrue(smic.accept(exVcapServices));

		SolaceMessagingInfo smi = smic.createServiceInfo(exVcapServices);

		// Validate smf is null. Others are not
		assertNull(smi.getSmfHost());
		assertEquals("tcps://192.168.1.50:7003,tcps://192.168.1.51:7003", smi.getSmfTlsHost());
	}

	@Test
	public void loadCreatorFromMeta() {

		String metaFileName = "src/main/resources/META-INF/services/org.springframework.cloud.cloudfoundry.CloudFoundryServiceInfoCreator";
		String solaceMessagingInfoCreatorClassName = null;
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader(metaFileName));

			while ((sCurrentLine = br.readLine()) != null) {
				 solaceMessagingInfoCreatorClassName = sCurrentLine;
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		try {
			Class<?> z = Class.forName(solaceMessagingInfoCreatorClassName);
			assertNotNull(z);
			assertEquals(z,SolaceMessagingInfoCreator.class);
		} catch (ClassNotFoundException e) {
			fail("Should not throw.");
		}

	}

	private Map<String,Object> createCredentialsMap() {

		Map<String, Object> exCred = new HashMap<String, Object>();

		exCred.put("clientUsername", "sample-client-username");
		exCred.put("clientPassword", "sample-client-password");
		exCred.put("msgVpnName", "sample-msg-vpn");
		exCred.put("smfHosts", Arrays.asList("tcp://192.168.1.50:7000"));
		exCred.put("smfTlsHosts", Arrays.asList("tcps://192.168.1.50:7003", "tcps://192.168.1.51:7003"));
		exCred.put("smfZipHosts", Arrays.asList("tcp://192.168.1.50:7001"));
		exCred.put("webMessagingUris", Arrays.asList("http://192.168.1.50:80"));
		exCred.put("webMessagingTlsUris", Arrays.asList("https://192.168.1.50:80"));
		exCred.put("jmsJndiUris", Arrays.asList("smf://192.168.1.50:7000"));
		exCred.put("jmsJndiTlsUris", Arrays.asList("smfs://192.168.1.50:7003", "smfs://192.168.1.51:7003"));
		exCred.put("mqttUris", Arrays.asList("tcp://192.168.1.50:7020"));
		exCred.put("mqttTlsUris", Arrays.asList("ssl://192.168.1.50:7021", "ssl://192.168.1.51:7021"));
		exCred.put("mqttWsUris", Arrays.asList("ws://192.168.1.50:7022"));
		exCred.put("mqttWssUris", Arrays.asList("wss://192.168.1.50:7023", "wss://192.168.1.51:7023"));
		exCred.put("restUris", Arrays.asList("http://192.168.1.50:7018"));
		exCred.put("restTlsUris", Arrays.asList("https://192.168.1.50:7019"));
		exCred.put("amqpUris", Arrays.asList("amqp://192.168.1.50:7016"));
		exCred.put("amqpTlsUris", Arrays.asList("amqps://192.168.1.50:7017"));
		exCred.put("managementHostnames", Arrays.asList("vmr-Medium-VMR-0"));
		exCred.put("managementUsername", "sample-mgmt-username");
		exCred.put("managementPassword", "sample-mgmt-password");
		exCred.put("activeManagementHostname", "vmr-medium-web");
		exCred.put("dmrClusterName", "dmr-cluster-name");
		exCred.put("dmrClusterPassword", "dmr-cluster-password");

		return exCred;
	}

	private Map<String, Object> createVcapMap() {
		return createVcapMap(solacePubSubLabel);
	}

	private Map<String, Object> createVcapMap(String label) {
		Map<String, Object> exVcapServices = new HashMap<String, Object>();

		Map<String, Object> exCred = createCredentialsMap();

		exVcapServices.put("credentials", exCred);
		exVcapServices.put("label", label);
		exVcapServices.put("name", "test-service-instance-name");
		exVcapServices.put("plan", "vmr-shared");
		exVcapServices.put("provider", "Solace Systems");
		// no need to check for tags in terms of validation. It's more for
		exVcapServices.put("tags", Arrays.asList("solace", "rest", "mqtt", "mq", "queue", "jms", "messaging", "amqp"));
		return exVcapServices;
	}

	private Map<String, Object> createCUPSVcapMap() {
		Map<String, Object> exVcapServices = new HashMap<String, Object>();
		Map<String, Object> exCred = createCredentialsMap();
		exVcapServices.put("credentials", exCred);

		exVcapServices.put("label", "user-provided");
		exVcapServices.put("name", "test-service-instance-name");
		exVcapServices.put("tags", Arrays.asList());
		return exVcapServices;
	}

	private void validateExampleSmi(SolaceMessagingInfo smi) {
		// Validate it all got set correctly.

		// Check Top Level stuff
		assertEquals("test-service-instance-name", smi.getId());
		assertEquals("sample-client-username", smi.getClientUsername());
		assertEquals("sample-client-password", smi.getClientPassword());
		assertEquals("sample-msg-vpn", smi.getMsgVpnName());

		// Check SMF
		assertEquals("tcp://192.168.1.50:7000", smi.getSmfHost());
		assertEquals("tcps://192.168.1.50:7003,tcps://192.168.1.51:7003", smi.getSmfTlsHost());
		assertEquals("tcp://192.168.1.50:7001", smi.getSmfZipHost());

		// Check JMS
		assertEquals("smf://192.168.1.50:7000", smi.getJmsJndiUri());
		assertEquals("smfs://192.168.1.50:7003,smfs://192.168.1.51:7003", smi.getJmsJndiTlsUri());

		// Check MQTT
		assertThat(smi.getMqttUris(), is(Arrays.asList("tcp://192.168.1.50:7020")));
		assertThat(smi.getMqttTlsUris(), is(Arrays.asList("ssl://192.168.1.50:7021", "ssl://192.168.1.51:7021")));
		assertThat(smi.getMqttWsUris(), is(Arrays.asList("ws://192.168.1.50:7022")));
		assertThat(smi.getMqttWssUris(), is(Arrays.asList("wss://192.168.1.50:7023", "wss://192.168.1.51:7023")));

		// Check REST
		assertThat(smi.getRestUris(), is(Arrays.asList("http://192.168.1.50:7018")));
		assertThat(smi.getRestTlsUris(), is(Arrays.asList("https://192.168.1.50:7019")));

		// Check AMQP
		assertThat(smi.getAmqpUris(), is(Arrays.asList("amqp://192.168.1.50:7016")));
		assertThat(smi.getAmqpTlsUris(), is(Arrays.asList("amqps://192.168.1.50:7017")));

		// Check Management Interfaces
		assertThat(smi.getManagementHostnames(), is(Arrays.asList("vmr-Medium-VMR-0")));
		assertEquals("sample-mgmt-username", smi.getManagementUsername());
		assertEquals("sample-mgmt-password", smi.getManagementPassword());
		assertEquals("vmr-medium-web", smi.getActiveManagementHostname());

		// Check DMR Clusters
		assertEquals("dmr-cluster-name", smi.getDmrClusterName());
		assertEquals("dmr-cluster-password", smi.getDmrClusterPassword());
	}
}
