/**
 * Copyright (c) 2016 Bosch Software Innovations GmbH.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *    Bosch Software Innovations GmbH - initial creation
 *
 */
package org.eclipse.hono.example;

import static java.net.HttpURLConnection.HTTP_ACCEPTED;
import static java.net.HttpURLConnection.HTTP_OK;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.PostConstruct;

import org.apache.qpid.proton.amqp.messaging.AmqpValue;
import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.amqp.messaging.Section;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.HonoClient;
import org.eclipse.hono.client.HonoClient.HonoClientBuilder;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.config.HonoClientConfigProperties;
import org.eclipse.hono.util.MessageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClientOptions;

/**
 * Example of a event/telemetry receiver that connects to the Hono Server, waits for incoming messages and logs the message
 * payload if anything is received.
 */
@Component
@Profile("!sender")
public class ExampleReceiver {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleReceiver.class);
    public static final String COMMAND = "command";

    @Value(value = "${tenant.id}")
    private String tenantId;

    @Autowired
    private HonoClientConfigProperties clientConfig;

    @Autowired
    private Environment environment;

    @Autowired
    private Vertx vertx;
    private HonoClient client;
    private final AtomicReference<Message> lastReceivedCommand = new AtomicReference<>();
    private MessageConsumer commandConsumer;

    @PostConstruct
    private void start() {

        final List<String> activeProfiles = Arrays.asList(environment.getActiveProfiles());
        client = HonoClientBuilder.newClient(clientConfig).vertx(vertx).build();
        final Future<MessageConsumer> startupTracker = Future.future();
        startupTracker.setHandler(done -> {
            if (done.succeeded()) {
                LOG.info("Receiver created successfully.");
                commandConsumer = done.result();
                vertx.executeBlocking(this::waitForInput, false, finish -> {
                    vertx.close();
                });
            } else {
                LOG.error("Error occurred during initialization of message receiver: {}", done.cause().getMessage());
                vertx.close();
            }
        });

        final Context ctx = vertx.getOrCreateContext();
        ctx.runOnContext((Void go) -> {
            /* step 1: connect hono client */
            final Future<HonoClient> connectionTracker = Future.future();
            client.connect(new ProtonClientOptions(), connectionTracker.completer());
            connectionTracker.compose(honoClient -> {
                /* step 2: wait for consumers */

                final Future<MessageConsumer> messageConsumer = Future.future();

                if (activeProfiles.contains("event")) {
                    client.createEventConsumer(tenantId,
                            msg -> handleMessage("event", msg),
                            messageConsumer.completer());
                } else if (activeProfiles.contains(COMMAND)) {
                    client.createCommandConsumer(tenantId,
                            msg -> handleMessage(COMMAND, msg),
                            messageConsumer.completer());
                } else {
                    messageConsumer.complete();

                    // default is telemetry consumer
                    client.createTelemetryConsumer(tenantId,
                            msg -> handleMessage("telemetry", msg),
                            messageConsumer.completer());
                }

                messageConsumer.setHandler(startupTracker.completer());

            }, startupTracker);
        });
    }

    private void waitForInput(final Future<Object> f) {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
        try {
            LOG.info("Press enter to stop receiver.");
            String input;
            do {
                input = reader.readLine();
                if (!input.isEmpty()) {
                    replyToCommand(input);
                }
            } while (!input.isEmpty());
            f.complete();
        } catch (final IOException e) {
            LOG.error("problem reading message from STDIN", e);
            f.fail(e);
        } finally {
            client.shutdown();
        }
    }

    private boolean replyToCommand(final String input) {
        if (lastReceivedCommand.get() == null) {
            LOG.info("Cannot reply, no command was received yet.");
            return false;
        }
        return commandConsumer.reply(lastReceivedCommand.get(), HTTP_OK, input);
    }

    private void handleMessage(final String endpoint, final Message msg) {
        final String deviceId = MessageHelper.getDeviceId(msg);
        final Section body = msg.getBody();
        String content = null;
        if (body instanceof Data) {
            content = ((Data) msg.getBody()).getValue().toString();
        } else if (body instanceof AmqpValue) {
            content = ((AmqpValue) msg.getBody()).getValue().toString();
        }

        LOG.info("received " + endpoint + " message [device: {}, content-type: {}]: {}", deviceId, msg.getContentType(), content);

        if (msg.getApplicationProperties() != null) {
            final Map props = msg.getApplicationProperties().getValue();
            LOG.info("... with application properties: {}", props);
        }

        if (COMMAND.equals(endpoint)) {
            lastReceivedCommand.set(msg);
            // reply with ACCEPTED message
            commandConsumer.reply(lastReceivedCommand.get(), HTTP_ACCEPTED, "message accepted for processing...");
            LOG.info("COMMAND received, enter message to reply!");
        }
    }
}
