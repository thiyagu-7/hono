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

package org.eclipse.hono.client.impl;

import java.util.Objects;
import java.util.function.Consumer;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.client.MessageSender;

import io.vertx.core.AsyncResult;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 * A Vertx-Proton based client for uploading telemtry data to a Hono server.
 */
public class CommandSenderImpl extends AbstractSender {

    private static final String COMMAND_ADDRESS_TEMPLATE = "command%s%s";
    private static final String COMMAND_REPLY_ADDRESS_TEMPLATE = "command-reply%s%s";

    private CommandSenderImpl(final Context context, final ProtonSender sender,final ProtonReceiver receiver) {
        super(context);
        this.receiver = receiver;
        this.sender = sender;
    }

    public static void create(
            final Context context,
            final ProtonConnection con,
            final String tenantId,
            final String pathSeparator,
            final Consumer<Message> replyConsumer,
            final Handler<AsyncResult<MessageSender>> creationHandler) {

        Objects.requireNonNull(context);
        Objects.requireNonNull(con);
        Objects.requireNonNull(tenantId);
        Objects.requireNonNull(pathSeparator);

        final Future<ProtonReceiver> consumerResult = Future.future();
        final Future<ProtonSender> senderResult = Future.future();

        createConsumer(context, con, COMMAND_REPLY_ADDRESS_TEMPLATE, tenantId, pathSeparator, replyConsumer).setHandler(consumerResult.completer());
        createSender(context, con, COMMAND_ADDRESS_TEMPLATE, tenantId, pathSeparator, ProtonQoS.AT_LEAST_ONCE).setHandler(senderResult.completer());

        CompositeFuture.all(consumerResult, senderResult).setHandler(created -> {
            if (created.succeeded()) {
                final ProtonReceiver receiver = (ProtonReceiver) created.result().list().get(0);
                final ProtonSender sender = (ProtonSender) created.result().list().get(1);
                creationHandler.handle(Future.succeededFuture(
                        new CommandSenderImpl(context, sender, receiver)));
            } else {
                creationHandler.handle(Future.failedFuture(created.cause()));
            }
        });
    }
}
