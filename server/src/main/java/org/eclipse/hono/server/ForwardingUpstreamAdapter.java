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
package org.eclipse.hono.server;

import java.util.Objects;

import org.apache.qpid.proton.amqp.transport.AmqpError;
import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.MessageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonHelper;
import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 *
 */
@Component
public abstract class ForwardingUpstreamAdapter extends BaseForwardingAdapter<ProtonReceiver> implements UpstreamAdapter {

    private final ReceiverFactory receiverFactory;

    /**
     * Creates a new adapter instance for a sender factory.
     *
     * @param vertx The Vert.x instance to run on.
     * @param receiverFactory The factory to use for creating new receivers for upstream command messages.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    protected ForwardingUpstreamAdapter(final Vertx vertx, final ReceiverFactory receiverFactory) {
        super(Objects.requireNonNull(vertx));
        this.receiverFactory = Objects.requireNonNull(receiverFactory);

        vertx.eventBus().consumer(Constants.EVENT_BUS_ADDRESS_CONNECTION_CLOSED,
                connectionClosed -> {
                    String connectionId = (String) connectionClosed.body();
                    onClientDisconnect(connectionId);
                });
    }

    @Override
    public final void onClientAttach(final UpstreamSender client, final Handler<AsyncResult<Void>> resultHandler) {
        createReceiver(
            client.getSourceAddress(),
            creationAttempt -> {
                if (creationAttempt.succeeded()) {
                    logger.info("created downstream receiver [con: {}, link: {}]", client.getConnectionId(), client.getLinkId());
                    final ProtonReceiver receiver = creationAttempt.result();
                    receiver.handler((delivery, message) -> processMessage(client, delivery, message));
                    client.handleFlow(receiver);

                    resultHandler.handle(Future.succeededFuture());
                } else {
                    resultHandler.handle(Future.failedFuture(creationAttempt.cause()));
                    logger.warn("can't create downstream sender [con: {}, link: {}]", client.getConnectionId(), client.getLinkId(), creationAttempt.cause());
                }
            });
    }

    @Override
    public final void processMessage(final UpstreamSender client, final ProtonDelivery delivery, final Message msg) {

        Objects.requireNonNull(client);
        Objects.requireNonNull(msg);
        Objects.requireNonNull(delivery);
//        if (client == null) {
//            logger.info("no upstream sender for link [{}] available, discarding message and closing link with client", client.getLinkId());
//            client.close(ErrorConditions.ERROR_NO_DOWNSTREAM_CONSUMER);
//        } else
        if (client.isOpen()) {
            forwardMessage(client.getSender(), msg, delivery);
        } else {
            logger.warn("downstream sender for link [{}] is not open, discarding message and closing link with client", client.getLinkId());
            client.close(ErrorConditions.ERROR_NO_DOWNSTREAM_CONSUMER);
            onClientDetach(client);
        }
    }

    private void createReceiver(
            final String targetAddress,
            final Handler<AsyncResult<ProtonReceiver>> handler) {

        final Future<ProtonReceiver> result = Future.future();
        result.setHandler(handler);
        if (downstreamConnection == null || downstreamConnection.isDisconnected()) {
            result.fail("downstream connection must be opened before creating sender");
        } else {
            final String address = targetAddress.replace(Constants.DEFAULT_PATH_SEPARATOR, honoConfig.getPathSeparator());
            receiverFactory.createReceiver(downstreamConnection, address, getDownstreamQos(), result);
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
