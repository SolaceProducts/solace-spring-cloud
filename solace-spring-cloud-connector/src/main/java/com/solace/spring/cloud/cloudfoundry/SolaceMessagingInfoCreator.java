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

import java.util.List;
import java.util.Map;

import org.springframework.cloud.cloudfoundry.CloudFoundryServiceInfoCreator;
import org.springframework.cloud.cloudfoundry.Tags;

import com.solace.spring.cloud.core.SolaceMessagingInfo;

public class SolaceMessagingInfoCreator extends CloudFoundryServiceInfoCreator<SolaceMessagingInfo> {

    // This creator will accept and parse any credentials that have the matching tag or label.
    // Therefore the default accept method is sufficient and doesn't need further specification.


    // Some URI properties are represented in VCAP_SERVICES as JSON arrays, but
    // the JCSMP Java client library expects them as comma-separated strings.
    // Therefore we do that transformation.

    static private String solacePubSubTag = "solace-pubsub";

    public SolaceMessagingInfoCreator() {
        super(new Tags(solacePubSubTag));
    }

    @SuppressWarnings("unchecked")
    @Override
    public SolaceMessagingInfo createServiceInfo(Map<String, Object> serviceData) {
        String id = getId(serviceData);

        String clientUsername = null;
        String clientPassword = null;
        String msgVpnName = null;
        List<String> smfHosts = null;
        List<String> smfTlsHosts = null;
        List<String> smfZipHosts = null;
        List<String> jmsJndiUris = null;
        List<String> jmsJndiTlsUris = null;
        List<String> restUris = null;
        List<String> restTlsUris = null;
        List<String> mqttUris = null;
        List<String> mqttTlsUris = null;
        List<String> amqpUris = null;
        List<String> amqpTlsUris = null;
        List<String> mqttWsUris = null;
        List<String> mqttWssUris = null;
        List<String> managementHostnames = null;
        String managementPassword = null;
        String managementUsername = null;
        String activeManagementHostname = null;
        String dmrClusterName = null;
        String dmrClusterPassword = null;

        Map<String, Object> credentials = getCredentials(serviceData);

        if (credentials == null) {
            throw new IllegalArgumentException("Received null credentials during object creation");
        }

        // Populate this the quick and dirty way for now. Can improve later as
        // we harden. As a start, we'll be tolerant of missing attributes and
        // just leave fields set to null.
        for (Map.Entry<String, Object> entry : credentials.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            switch (key) {
                case "clientUsername":
                    clientUsername = (String) value;
                    break;
                case "clientPassword":
                    clientPassword = (String) value;
                    break;
                case "msgVpnName":
                    msgVpnName = (String) value;
                    break;
                case "smfHosts":
                    smfHosts = (List<String>) value;
                    break;
                case "smfTlsHosts":
                    smfTlsHosts = (List<String>) value;
                    break;
                case "smfZipHosts":
                    smfZipHosts = (List<String>) value;
                    break;
                case "jmsJndiUris":
                    jmsJndiUris = (List<String>) value;
                    break;
                case "jmsJndiTlsUris":
                    jmsJndiTlsUris = (List<String>) value;
                    break;
                case "managementUsername":
                    managementUsername = (String) value;
                    break;
                case "managementPassword":
                    managementPassword = (String) value;
                    break;
                case "activeManagementHostname":
                    activeManagementHostname = (String) value;
                    break;
                case "restUris":
                    restUris = (List<String>) value;
                    break;
                case "restTlsUris":
                    restTlsUris = (List<String>) value;
                    break;
                case "mqttUris":
                    mqttUris = (List<String>) value;
                    break;
                case "mqttTlsUris":
                    mqttTlsUris = (List<String>) value;
                    break;
                case "mqttWsUris":
                    mqttWsUris = (List<String>) value;
                    break;
                case "mqttWssUris":
                    mqttWssUris = (List<String>) value;
                    break;
                case "amqpUris":
                    amqpUris = (List<String>) value;
                    break;
                case "amqpTlsUris":
                    amqpTlsUris = (List<String>) value;
                    break;
                case "managementHostnames":
                    managementHostnames = (List<String>) value;
                    break;
                case "dmrClusterName":
                    dmrClusterName = (String) value;
                    break;
                case "dmrClusterPassword":
                    dmrClusterPassword = (String) value;
            }
        }


        // Convert Lists to comma-separated strings on these properties, to be compatible with  the JCSMP Java client library.
        String smfHost = null;
        String smfTlsHost = null;
        String smfZipHost = null;
        String jmsJndiUri = null;
        String jmsJndiTlsUri = null;


        if (smfHosts != null) smfHost = String.join(",", smfHosts);
        if (smfTlsHosts != null) smfTlsHost = String.join(",", smfTlsHosts);
        if (smfZipHosts != null) smfZipHost = String.join(",", smfZipHosts);
        if (jmsJndiUris != null) jmsJndiUri = String.join(",", jmsJndiUris);
        if (jmsJndiTlsUris != null) jmsJndiTlsUri = String.join(",", jmsJndiTlsUris);

        return new SolaceMessagingInfo(id, clientUsername, clientPassword, msgVpnName,
                smfHost, smfTlsHost, smfZipHost, jmsJndiUri, jmsJndiTlsUri, restUris, restTlsUris, mqttUris,
                mqttTlsUris, mqttWsUris, mqttWssUris, amqpUris, amqpTlsUris, managementHostnames, managementPassword, managementUsername, activeManagementHostname,
                dmrClusterName, dmrClusterPassword);
    }


	public boolean accept(Map<String, Object> serviceData) {
		return super.accept(serviceData) || (isUserProvided(serviceData) && isSolaceMessagingInfo(serviceData));
	}

	public boolean isUserProvided(Map<String, Object> serviceData) {
		if( serviceData == null )
			return false;

		String label = (String) serviceData.get("label");
		if( "user-provided".startsWith(label)) {
			return true;
		}
		return false;
	}

	public boolean isSolaceMessagingInfo(Map<String, Object> serviceData) {
		if( serviceData == null )
			return false;

		try {
			SolaceMessagingInfo solMessagingInfo = createServiceInfo(serviceData);
			// Good enough test
			// Don't check for client or management username/password as they can be missing when using LDAP
			// Not checking other Uri(s) especially that this can be a user-provided service.
			if( solMessagingInfo != null && solMessagingInfo.getId() != null && solMessagingInfo.getMsgVpnName() != null )
				return true;

		} catch(Throwable t) {
			return false;
		}

		return false;
	}

}