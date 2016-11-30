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

package org.eclipse.hono.command.impl;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.server.ForwardingLink;
import org.eclipse.hono.server.UpstreamAdapter;
import org.eclipse.hono.server.UpstreamSender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.impl.ProtonDeliveryImpl;

/**
 *
 */
public class SimpleUpstreamAdapter implements UpstreamAdapter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SimpleUpstreamAdapter.class);

    private UpstreamSender sender;

    @Override
    public void start(final Future<Void> startFuture) {
        LOGGER.info("Starting test adapter");
        startFuture.complete();
    }

    @Override
    public void stop(final Future<Void> stopFuture) {
        LOGGER.info("Stopping test adapter");
        stopFuture.complete();
    }

    @Override
    public void onClientAttach(final UpstreamSender client, final Handler<AsyncResult<Void>> resultHandler) {
        this.sender = client;
        resultHandler.handle(Future.succeededFuture());
    }

    @Override
    public void onClientDetach(final ForwardingLink client) {
        LOGGER.info("Client detach...");
    }

    @Override
    public void onClientDisconnect(final String connectionId) {
        LOGGER.info("Client disconnected...");
    }

    @Override public void processMessage(final UpstreamSender client, final ProtonDelivery delivery, final Message message) {

        client.getSender().send(message);
    }

    public void send(final ProtonDelivery delivery, final Message message) {
        processMessage(sender, delivery, message);
    }
}
