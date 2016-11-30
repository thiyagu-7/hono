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
 */
package org.eclipse.hono.server;

import java.util.Objects;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.util.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;

/**
 * A downstream adapter that provides support for sending messages to an AMQP 1.0 container.
 *
 */
@Component
public abstract class ForwardingDownstreamAdapter extends BaseForwardingAdapter<ProtonSender> implements DownstreamAdapter {

    protected Logger logger = LoggerFactory.getLogger(getClass());
    private final SenderFactory senderFactory;

    /**
     * Creates a new adapter instance for a sender factory.
     * 
     * @param vertx The Vert.x instance to run on.
     * @param senderFactory The factory to use for creating new senders for downstream telemetry data.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    protected ForwardingDownstreamAdapter(final Vertx vertx, final SenderFactory senderFactory) {
        super(Objects.requireNonNull(vertx));
        this.senderFactory = Objects.requireNonNull(senderFactory);

        vertx.eventBus().consumer(Constants.EVENT_BUS_ADDRESS_CONNECTION_CLOSED,
                connectionClosed -> {
                    String connectionId = (String) connectionClosed.body();
                    onClientDisconnect(connectionId);
                });

    }

    @Override
    public final void onClientAttach(final UpstreamReceiver client, final Handler<AsyncResult<Void>> resultHandler) {

        if (containsLink(client.getLinkId())) {
            logger.info("reusing existing downstream sender [con: {}, link: {}]", client.getConnectionId(), client.getLinkId());
        } else {
            createSender(
                    client.getTargetAddress(),
                    replenishedSender -> {/* handleFlow(replenishedSender, client) */},
                    creationAttempt -> {
                        if (creationAttempt.succeeded()) {
                            logger.info("created downstream sender [con: {}, link: {}]", client.getConnectionId(), client.getLinkId());
                            final ProtonSender sender = creationAttempt.result();
                            client.handleFlow(sender);
                            addLink(client.getConnectionId(), client.getLinkId(), sender);
                            resultHandler.handle(Future.succeededFuture());
                        } else {
                            resultHandler.handle(Future.failedFuture(creationAttempt.cause()));
                            logger.warn("can't create downstream sender [con: {}, link: {}]", client.getConnectionId(), client.getLinkId(), creationAttempt.cause());
                        }
                    });
        }
    }

    private void createSender(
            final String targetAddress,
            final Handler<ProtonSender> sendQueueDrainHandler,
            final Handler<AsyncResult<ProtonSender>> handler) {

        Future<ProtonSender> result = Future.future();
        result.setHandler(handler);
        if (downstreamConnection == null || downstreamConnection.isDisconnected()) {
            result.fail("downstream connection must be opened before creating sender");
        } else {
            String address = targetAddress.replace(Constants.DEFAULT_PATH_SEPARATOR, honoConfig.getPathSeparator());
            senderFactory.createSender(downstreamConnection, address, getDownstreamQos(), sendQueueDrainHandler, result);
        }
    }

    @Override
    public final void processMessage(final UpstreamReceiver client, final ProtonDelivery delivery, final Message msg) {

        Objects.requireNonNull(client);
        Objects.requireNonNull(msg);
        Objects.requireNonNull(delivery);
        final ProtonSender sender = getLink(client.getLinkId());
        if (sender == null) {
            logger.info("no downstream sender for link [{}] available, discarding message and closing link with client", client.getLinkId());
            client.close(ErrorConditions.ERROR_NO_DOWNSTREAM_CONSUMER);
        } else if (sender.isOpen()) {
            forwardMessage(sender, msg, delivery);
        } else {
            logger.warn("downstream sender for link [{}] is not open, discarding message and closing link with client", client.getLinkId());
            client.close(ErrorConditions.ERROR_NO_DOWNSTREAM_CONSUMER);
            onClientDetach(client);
        }
    }

    /**
     * Forwards the message to the downstream container.
     * <p>
     * It is the implementer's responsibility to handle message disposition and settlement with the upstream
     * client using the <em>delivery</em> object.
     * 
     * @param sender The link to the downstream container.
     * @param msg The message to send.
     * @param delivery The handle for settling the message with the client.
     */
    protected abstract void forwardMessage(final ProtonSender sender, final Message msg, final ProtonDelivery delivery);

    /**
     * Gets the Quality-of-Service type used for the link with the downstream container.
     * 
     * @return The QoS.
     */
    protected abstract ProtonQoS getDownstreamQos();
}
