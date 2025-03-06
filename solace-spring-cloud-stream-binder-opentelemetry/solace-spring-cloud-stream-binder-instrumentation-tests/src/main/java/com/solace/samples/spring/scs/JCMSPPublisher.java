package com.solace.samples.spring.scs;

import com.solacesystems.jcsmp.BytesXMLMessage;
import com.solacesystems.jcsmp.DeliveryMode;
import com.solacesystems.jcsmp.JCSMPException;
import com.solacesystems.jcsmp.JCSMPFactory;
import com.solacesystems.jcsmp.JCSMPProperties;
import com.solacesystems.jcsmp.JCSMPSession;
import com.solacesystems.jcsmp.JCSMPStreamingPublishEventHandler;
import com.solacesystems.jcsmp.SDTMap;
import com.solacesystems.jcsmp.Topic;
import com.solacesystems.jcsmp.XMLContentMessage;
import com.solacesystems.jcsmp.XMLMessage;
import com.solacesystems.jcsmp.XMLMessageProducer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class JCMSPPublisher {

  public static void main(String[] args) {
    // Solace JCSMP Message Publisher

    // Create a JCSMP session
    final JCSMPProperties properties = new JCSMPProperties();
    properties.setProperty(JCSMPProperties.HOST, "tcp://localhost:45555"); // Solace messaging host
    properties.setProperty(JCSMPProperties.VPN_NAME, "default"); // Solace VPN name
    properties.setProperty(JCSMPProperties.USERNAME, "default"); // Solace username
    properties.setProperty(JCSMPProperties.PASSWORD, "default"); // Solace password

    try {
      final JCSMPSession session = JCSMPFactory.onlyInstance().createSession(properties);
      session.connect();

      // Create a message producer
      final XMLMessageProducer producer = session.getMessageProducer(new JCSMPStreamingPublishEventHandler() {
        @Override
        public void responseReceived(String messageID) {
          System.out.println("Producer received response for msg: " + messageID);
        }

        @Override
        public void handleError(String messageID, JCSMPException e, long timestamp) {
          System.out.printf("Producer received error for msg: %s@%s - %s%n", messageID, timestamp, e);
        }
      });

      final String msgString = "{\"timestamp\":1731960166002,\"sensorID\":\"3a0ef4a4-d2f0-409f-acac-209a27290699\",\"temperature\":82.92734680001152,\"baseUnit\":\"FAHRENHEIT\"}";

      // Create a topic
      final Topic topic = JCSMPFactory.onlyInstance().createTopic("sensor/temperature/fahrenheit");
      BytesXMLMessage msg = JCSMPFactory.onlyInstance().createMessage(XMLContentMessage.class);
      msg.setStructuredMsgType(XMLMessage.UNSTRUCTURED_XML_MESSAGE);
      msg.setDeliveryMode(DeliveryMode.PERSISTENT);

      long id = System.currentTimeMillis();
      msg.setCorrelationKey(id);
      String message_identifier = UUID.randomUUID().toString();
      msg.setApplicationMessageId(message_identifier);

      //msg.setHTTPContentType("application/json");

      //SDTMap headerMap = JCSMPFactory.onlyInstance().createMap();
      //headerMap.putString("contentType", "application/json");
      //msg.setProperties(headerMap);

      msg.writeBytes(msgString.getBytes(StandardCharsets.UTF_8)); //This is wrong call.

      producer.send(msg, topic);

      System.out.println("Message sent. Exiting.");

      // Close the session
      session.closeSession();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
