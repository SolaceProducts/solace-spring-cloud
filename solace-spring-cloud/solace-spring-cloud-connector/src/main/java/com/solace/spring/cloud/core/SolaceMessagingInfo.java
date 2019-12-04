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

import java.util.List;

import com.solace.services.core.model.SolaceServiceCredentials;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.cloud.service.BaseServiceInfo;
import org.springframework.cloud.service.ServiceInfo.ServiceLabel;

/*
 * A implementation of Spring Cloud Connector ServiceInfo to wrap the SolaceMessaging Cloud Foundry
 * Service. This class provides easy access to all of the information in the VCAP_SERVICES without
 * extra dependencies on any Solace Enterprise APIs.
 *
 * For more details see the GitHub project:
 *    - https://github.com/SolaceProducts/sl-spring-cloud-connectors
 *
 */
@ServiceLabel("solacepubsub")
public class SolaceMessagingInfo extends BaseServiceInfo implements SolaceServiceCredentials {

	private String clientUsername;
	private String clientPassword;
	private String msgVpnName;
	private String smfHost;
	private String smfTlsHost;
	private String smfZipHost;
	private String jmsJndiUri;
	private String jmsJndiTlsUri;
	private List<String> restUris;
	private List<String> restTlsUris;
	private List<String> amqpUris;
	private List<String> amqpTlsUris;
	private List<String> mqttUris;
	private List<String> mqttTlsUris;
	private List<String> mqttWsUris;
	private List<String> mqttWssUris;
	private List<String> managementHostnames;
	private String managementPassword;
	private String managementUsername;
	private String activeManagementHostname;
	private String dmrClusterName;
	private String dmrClusterPassword;


	// Default constructor to enable bean unit testing.
	public SolaceMessagingInfo() {
		super(null);
	}

	public SolaceMessagingInfo(String id, String clientUsername, String clientPassword, String msgVpnName,
			String smfHost, String smfTlsHost, String smfZipHost, String jmsJndiUri, String jmsJndiTlsUri,
			List<String> restUris, List<String> restTlsUris, List<String> mqttUris, List<String> mqttTlsUris,
			List<String> mqttWsUris, List<String> mqttWssUris, List<String> amqpUris, List<String> amqpTlsUris, List<String> managementHostnames,
			String managementPassword, String managementUsername, String activeManagementHostname,
							   String dmrClusterName, String dmrClusterPassword) {
		super(id);
		this.clientUsername = clientUsername;
		this.clientPassword = clientPassword;
		this.msgVpnName = msgVpnName;
		this.smfHost = smfHost;
		this.smfTlsHost = smfTlsHost;
		this.smfZipHost = smfZipHost;
		this.jmsJndiUri = jmsJndiUri;
		this.jmsJndiTlsUri = jmsJndiTlsUri;
		this.restUris = restUris;
		this.restTlsUris = restTlsUris;
		this.mqttUris = mqttUris;
		this.mqttTlsUris = mqttTlsUris;
		this.mqttWsUris = mqttWsUris;
		this.mqttWssUris = mqttWssUris;
		this.amqpUris = amqpUris;
		this.amqpTlsUris = amqpTlsUris;
		this.managementHostnames = managementHostnames;
		this.managementPassword = managementPassword;
		this.managementUsername = managementUsername;
		this.activeManagementHostname = activeManagementHostname;
		this.dmrClusterName = dmrClusterName;
		this.dmrClusterPassword = dmrClusterPassword;
	}



	/**
	 * @return the clientUsername
	 */
	@Override
	@ServiceProperty
	public String getClientUsername() {
		return clientUsername;
	}

	/**
	 * @return the clientPassword
	 */
	@Override
	@ServiceProperty
	public String getClientPassword() {
		return clientPassword;
	}

	/**
	 * @return the msgVpnName
	 */
	@Override
	@ServiceProperty
	public String getMsgVpnName() {
		return msgVpnName;
	}

	/**
	 * @return the smfHost
	 */
	@Override
	@ServiceProperty
	public String getSmfHost() {
		return smfHost;
	}

	/**
	 * @return the smfTlsHost
	 */
	@Override
	@ServiceProperty
	public String getSmfTlsHost() {
		return smfTlsHost;
	}

	/**
	 * @return the smfZipHost
	 */
	@Override
	@ServiceProperty
	public String getSmfZipHost() {
		return smfZipHost;
	}

	/**
	 * @return the jmsJndiUri
	 */
	@Override
	@ServiceProperty
	public String getJmsJndiUri() {
		return jmsJndiUri;
	}

	/**
	 * @return the jmsJndiTlsUri
	 */
	@Override
	@ServiceProperty
	public String getJmsJndiTlsUri() {
		return jmsJndiTlsUri;
	}

	/**
	 * @return the restUris
	 */
	@Override
	@ServiceProperty
	public List<String> getRestUris() {
		return restUris;
	}

	/**
	 * @return the restTlsUris
	 */
	@Override
	@ServiceProperty
	public List<String> getRestTlsUris() {
		return restTlsUris;
	}

	/**
	 * @return the amqpUris
	 */
	@Override
	@ServiceProperty
	public List<String> getAmqpUris() {
		return amqpUris;
	}

	/**
	 * @return the amqpTlsUris
	 */
	@Override
	@ServiceProperty
	public List<String> getAmqpTlsUris() {
		return amqpTlsUris;
	}

	/**
	 * @return the mqttUris
	 */
	@Override
	@ServiceProperty
	public List<String> getMqttUris() {
		return mqttUris;
	}

	/**
	 * @return the mqttTlsUris
	 */
	@Override
	@ServiceProperty
	public List<String> getMqttTlsUris() {
		return mqttTlsUris;
	}

	/**
	 * @return the mqttWsUris
	 */
	@Override
	@ServiceProperty
	public List<String> getMqttWsUris() {
		return mqttWsUris;
	}

	/**
	 * @return the mqttWssUris
	 */
	@Override
	@ServiceProperty
	public List<String> getMqttWssUris() {
		return mqttWssUris;
	}


	/**
	 * @return the managementHostnames
	 */
	@Override
	@ServiceProperty
	public List<String> getManagementHostnames() {
		return managementHostnames;
	}

	/**
	 * @return the managementPassword
	 */
	@Override
	@ServiceProperty
	public String getManagementPassword() {
		return managementPassword;
	}

	/**
	 * @return the managementUsername
	 */
	@Override
	@ServiceProperty
	public String getManagementUsername() {
		return managementUsername;
	}

    /**
     * @return the activeManagementHostname
     */
	@Override
    @ServiceProperty
    public String getActiveManagementHostname() {
        return activeManagementHostname;
    }

	/**
	 * @return the dmrClusterName
	 */
	@Override
	@ServiceProperty
	public String getDmrClusterName() {
		return dmrClusterName;
	}

	/**
	 * @return the dmrClusterPassword
	 */
	@Override
	@ServiceProperty
	public String getDmrClusterPassword() {
		return dmrClusterPassword;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return ToStringBuilder.reflectionToString(this);
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		result = prime * result + ((amqpTlsUris == null) ? 0 : amqpTlsUris.hashCode());
		result = prime * result + ((amqpUris == null) ? 0 : amqpUris.hashCode());
		result = prime * result + ((clientPassword == null) ? 0 : clientPassword.hashCode());
		result = prime * result + ((clientUsername == null) ? 0 : clientUsername.hashCode());
		result = prime * result + ((jmsJndiTlsUri == null) ? 0 : jmsJndiTlsUri.hashCode());
		result = prime * result + ((jmsJndiUri == null) ? 0 : jmsJndiUri.hashCode());
		result = prime * result + ((managementHostnames == null) ? 0 : managementHostnames.hashCode());
		result = prime * result + ((managementPassword == null) ? 0 : managementPassword.hashCode());
		result = prime * result + ((managementUsername == null) ? 0 : managementUsername.hashCode());
		result = prime * result + ((activeManagementHostname == null) ? 0 : activeManagementHostname.hashCode());
		result = prime * result + ((dmrClusterName == null) ? 0 : dmrClusterName.hashCode());
		result = prime * result + ((dmrClusterPassword == null) ? 0 : dmrClusterPassword.hashCode());
		result = prime * result + ((mqttTlsUris == null) ? 0 : mqttTlsUris.hashCode());
		result = prime * result + ((mqttUris == null) ? 0 : mqttUris.hashCode());
		result = prime * result + ((mqttWsUris == null) ? 0 : mqttWsUris.hashCode());
		result = prime * result + ((mqttWssUris == null) ? 0 : mqttWssUris.hashCode());
		result = prime * result + ((msgVpnName == null) ? 0 : msgVpnName.hashCode());
		result = prime * result + ((restTlsUris == null) ? 0 : restTlsUris.hashCode());
		result = prime * result + ((restUris == null) ? 0 : restUris.hashCode());
		result = prime * result + ((smfTlsHost == null) ? 0 : smfTlsHost.hashCode());
		result = prime * result + ((smfHost == null) ? 0 : smfHost.hashCode());
		result = prime * result + ((smfZipHost == null) ? 0 : smfZipHost.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 *
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		SolaceMessagingInfo other = (SolaceMessagingInfo) obj;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		if (amqpTlsUris == null) {
			if (other.amqpTlsUris != null)
				return false;
		} else if (!amqpTlsUris.equals(other.amqpTlsUris))
			return false;
		if (amqpUris == null) {
			if (other.amqpUris != null)
				return false;
		} else if (!amqpUris.equals(other.amqpUris))
			return false;
		if (clientPassword == null) {
			if (other.clientPassword != null)
				return false;
		} else if (!clientPassword.equals(other.clientPassword))
			return false;
		if (clientUsername == null) {
			if (other.clientUsername != null)
				return false;
		} else if (!clientUsername.equals(other.clientUsername))
			return false;
		if (jmsJndiTlsUri == null) {
			if (other.jmsJndiTlsUri != null)
				return false;
		} else if (!jmsJndiTlsUri.equals(other.jmsJndiTlsUri))
			return false;
		if (jmsJndiUri == null) {
			if (other.jmsJndiUri != null)
				return false;
		} else if (!jmsJndiUri.equals(other.jmsJndiUri))
			return false;
		if (managementHostnames == null) {
			if (other.managementHostnames != null)
				return false;
		} else if (!managementHostnames.equals(other.managementHostnames))
			return false;
		if (managementPassword == null) {
			if (other.managementPassword != null)
				return false;
		} else if (!managementPassword.equals(other.managementPassword))
			return false;
		if (managementUsername == null) {
			if (other.managementUsername != null)
				return false;
		} else if (!managementUsername.equals(other.managementUsername))
			return false;
        if (activeManagementHostname == null) {
            if (other.activeManagementHostname != null)
                return false;
        } else if (!activeManagementHostname.equals(other.activeManagementHostname))
            return false;
		if (dmrClusterName == null) {
			if (other.dmrClusterName != null)
				return false;
		} else if (!dmrClusterName.equals(other.dmrClusterName))
			return false;
		if (dmrClusterPassword == null) {
			if (other.dmrClusterPassword != null)
				return false;
		} else if (!dmrClusterPassword.equals(other.dmrClusterPassword))
			return false;
		if (mqttTlsUris == null) {
			if (other.mqttTlsUris != null)
				return false;
		} else if (!mqttTlsUris.equals(other.mqttTlsUris))
			return false;
		if (mqttUris == null) {
			if (other.mqttUris != null)
				return false;
		} else if (!mqttUris.equals(other.mqttUris))
			return false;
		if (mqttWsUris == null) {
			if (other.mqttWsUris != null)
				return false;
		} else if (!mqttWsUris.equals(other.mqttWsUris))
			return false;
		if (mqttWssUris == null) {
			if (other.mqttWssUris != null)
				return false;
		} else if (!mqttWssUris.equals(other.mqttWssUris))
			return false;
		if (msgVpnName == null) {
			if (other.msgVpnName != null)
				return false;
		} else if (!msgVpnName.equals(other.msgVpnName))
			return false;
		if (restTlsUris == null) {
			if (other.restTlsUris != null)
				return false;
		} else if (!restTlsUris.equals(other.restTlsUris))
			return false;
		if (restUris == null) {
			if (other.restUris != null)
				return false;
		} else if (!restUris.equals(other.restUris))
			return false;
		if (smfTlsHost == null) {
			if (other.smfTlsHost != null)
				return false;
		} else if (!smfTlsHost.equals(other.smfTlsHost))
			return false;
		if (smfHost == null) {
			if (other.smfHost != null)
				return false;
		} else if (!smfHost.equals(other.smfHost))
			return false;
		if (smfZipHost == null) {
			if (other.smfZipHost != null)
				return false;
		} else if (!smfZipHost.equals(other.smfZipHost))
			return false;
		return true;
	}

	public boolean isHA(){
		Boolean bool = false;
		if (smfHost != null) {
			bool = smfHost.contains(",");
		}
		return bool;
	}

}
