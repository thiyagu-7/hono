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

package org.eclipse.hono.tests.client;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.apache.qpid.proton.amqp.messaging.Data;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.HonoClient;
import org.eclipse.hono.client.MessageConsumer;
import org.eclipse.hono.client.MessageSender;
import org.eclipse.hono.util.MessageHelper;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

/**
 * A simple test that uses the {@code HonoClient} to send some event messages to Hono server and verifies they are forwarded to the downstream host
 * via the configured Artemis broker.
 */
@RunWith(VertxUnitRunner.class)
public class CommandClientIT extends ClientTestBase {

    private final AtomicReference<MessageConsumer> replyHandler = new AtomicReference<>();

    @Override
    void createConsumer(final TestContext ctx, final String tenantId, final Consumer<Message> messageConsumer, final Handler<AsyncResult<MessageConsumer>> setupTracker) {

        // intercept message consumer
        final AtomicReference<MessageConsumer> commandConsumer = new AtomicReference<>();
        final Future<MessageConsumer> future = Future.future();
        future.setHandler(ar -> {
            if (ar.succeeded()) {
                commandConsumer.set(ar.result());
            }
            setupTracker.handle(ar);
        });

        // wrap message consumer
        final Consumer<Message> consumer = cmd -> {
            LOGGER.info("Command Receiver received command {}: {}", cmd.getMessageId(), new String(((Data)cmd.getBody()).getValue().getArray()));
            commandConsumer.get().reply(cmd, 200, new String(((Data)cmd.getBody()).getValue().getArray()).toUpperCase());
            messageConsumer.accept(cmd);
        };

        honoClient.createCommandConsumer(tenantId, consumer, future.completer());
    }

    @Override
    void createProducer(final TestContext ctx, final String tenantId, final Handler<AsyncResult<MessageSender>> setupTracker) {
        honoClient.getOrCreateCommandSender(tenantId, reply -> replyConsumer.get().accept(reply), setupTracker);
    }

    @Override
    protected void prepareSending(final TestContext ctx) {

        final Async replied = ctx.async(MSG_COUNT);
        replyConsumer.set(reply -> {
            replied.countDown();
            LOGGER.info("Command Sender received reply to {}: {}", reply.getCorrelationId(), new String(((Data)reply.getBody()).getValue().getArray()));
        });
    }

    @Override
    protected void assertMessagePropertiesArePresent(final TestContext ctx, final Message msg) {
        super.assertMessagePropertiesArePresent(ctx, msg);
        if (Boolean.getBoolean(CHECK_ARTEMIS_HEADER_PROPERTY)) {
            // assert that the message was routed via Artemis by checking for the _AMQ_VALIDATED_USER field which is populated by Artemis
            ctx.assertNotNull(MessageHelper.getApplicationProperty(msg.getApplicationProperties(), "_AMQ_VALIDATED_USER", String.class));
        }
    }
}
