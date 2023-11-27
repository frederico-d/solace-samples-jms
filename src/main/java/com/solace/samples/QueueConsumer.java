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
 *  Solace JMS 1.1 Examples: QueueConsumer
 */

package com.solace.samples;

import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import com.solacesystems.jms.SolConnectionFactory;
import com.solacesystems.jms.SolJmsUtility;
import com.solacesystems.jms.SupportedProperty;

/**
 * Receives a persistent message from a queue using Solace JMS API
 * implementation.
 *
 * The queue used for messages is created on the message broker.
 */
public class QueueConsumer {

    private static String HOST;
    private static String VPN;
    private static String USERNAME;
    private static String PASSWORD;
    private static String QUEUE_NAME;

    // Latch used for synchronizing between threads
    final CountDownLatch latch = new CountDownLatch(1);

    public void run() throws Exception {

        System.out.printf("QueueConsumer is connecting to Solace messaging at %s...%n", HOST);

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

        // Create a non-transacted, client ACK session.
        Session session = connection.createSession(false, SupportedProperty.SOL_CLIENT_ACKNOWLEDGE);

        System.out.printf("Connected to the Solace Message VPN '%s' with client username '%s'.%n", VPN,
                USERNAME);

        // Create the queue programmatically and the corresponding router resource
        // will also be created dynamically because DynamicDurables is enabled.
        Queue queue = session.createQueue(QUEUE_NAME);

        // From the session, create a consumer for the destination.
        MessageConsumer messageConsumer = session.createConsumer(queue);

        // Use the anonymous inner class for receiving messages asynchronously
        messageConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    if (message instanceof TextMessage) {
                        System.out.printf("TextMessage received: '%s'%n", ((TextMessage) message).getText());
                    } else {
                        System.out.println("Message received.");
                    }
                    System.out.printf("Message Content:%n%s%n", SolJmsUtility.dumpMessage(message));

                    // ACK the received message manually because of the set
                    // SupportedProperty.SOL_CLIENT_ACKNOWLEDGE above
                    message.acknowledge();

                    latch.countDown(); // unblock the main thread
                } catch (JMSException ex) {
                    System.out.println("Error processing incoming message.");
                    ex.printStackTrace();
                }
            }
        });

        // Start receiving messages
        connection.start();
        System.out.println("Awaiting message...");
        // the main thread blocks at the next statement until a message received
        latch.await();

        connection.stop();
        // Close everything in the order reversed from the opening order
        // NOTE: as the interfaces below extend AutoCloseable,
        // with them it's possible to use the "try-with-resources" Java statement
        // see details at
        // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
        messageConsumer.close();
        session.close();
        connection.close();
    }

    private QueueConsumer loadProperties() {
        Properties prop = new Properties();
        try (InputStream input = QueueConsumer.class.getClassLoader().getResourceAsStream("config.properties")) {
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
        String host = "tcps://broker.dev.apigw.jbinfra-nonprod.net:55443";
        String vpn = "sfl";
        String clientUsername = "sfl-consumer";
        String password = "z3qCix6pX1gPoye5rHx9cDQhTnUGzMFV";

        new QueueConsumer().loadProperties().run();
    }

}
