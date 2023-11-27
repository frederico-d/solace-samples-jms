/**
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

/**
 *  Solace JMS 1.1 Examples: QueueProducer
 */

package com.solace.samples;

import java.io.InputStream;
import java.util.Properties;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;

/**
 * Sends a persistent message to a queue using Solace JMS API implementation.
 * 
 * The queue used for messages is created on the message broker.
 */
public class QueueProducer {

    private static String HOST;
    private static String VPN;
    private static String USERNAME;
    private static String PASSWORD;
    private static String QUEUE_NAME;

    public void run() throws Exception {

        System.out.printf("QueueProducer is connecting to Solace messaging at %s...%n", HOST);

        // Programmatically create the connection factory using default settings
        SolConnectionFactory connectionFactory = SolJmsUtility.createConnectionFactory();
        connectionFactory.setHost(HOST);
        connectionFactory.setVPN(VPN);
        connectionFactory.setUsername(USERNAME);
        connectionFactory.setPassword(PASSWORD);

        // Enables persistent queues or topic endpoints to be created dynamically
        // on the router, used when Session.createQueue() is called below
        connectionFactory.setDynamicDurables(true);

        // Create connection to the Solace router
        Connection connection = connectionFactory.createConnection();

        // Create a non-transacted, auto ACK session.
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        System.out.printf("Connected to the Solace Message VPN '%s' with client username '%s'.%n", VPN,
                USERNAME);

        // Create the queue programmatically and the corresponding router resource
        // will also be created dynamically because DynamicDurables is enabled.
        Queue queue = session.createQueue(QUEUE_NAME);

        // Create the message producer for the created queue
        MessageProducer messageProducer = session.createProducer(queue);

        // Send the message
        // NOTE: JMS Message Priority is not supported by the Solace Message Bus
        for (int i = 0; i < 1000; i++)
            producePersistentMessage(messageProducer, queue, session);

        // Close everything in the order reversed from the opening order
        // NOTE: as the interfaces below extend AutoCloseable,
        // with them it's possible to use the "try-with-resources" Java statement
        // see details at
        // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
        messageProducer.close();
        session.close();
        connection.close();
    }

    private void producePersistentMessage(MessageProducer messageProducer, Queue queue, Session session)
            throws JMSException {
        // Create a text message.
        TextMessage messagePersistent = session.createTextMessage("Persistent message");

        // System.out.printf("Sending Persistent message '%s' to queue '%s'...%n",
        // messagePersistent.getText(),queue.toString());
        messageProducer.send(queue, messagePersistent, DeliveryMode.PERSISTENT, Message.DEFAULT_PRIORITY,
                Message.DEFAULT_TIME_TO_LIVE);

        // System.out.println("Sent Persistent message successfully. Exiting...");
    }

    private QueueProducer loadProperties() {
        Properties prop = new Properties();
        try (InputStream input = QueueProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return null;
            }
            prop.load(input);
            // Access properties using getProperty method
            HOST = prop.getProperty("host");
            VPN = prop.getProperty("vpn");
            USERNAME = prop.getProperty("clientUsername");
            PASSWORD = prop.getProperty("password");
            QUEUE_NAME = prop.getProperty("queueName");

        } catch (Exception e) {
            e.printStackTrace();
        }

        return this;
    }

    public static void main(String... args) throws Exception {

        new QueueProducer().loadProperties().run();
    }

}
