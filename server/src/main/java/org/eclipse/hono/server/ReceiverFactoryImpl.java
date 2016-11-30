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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;
import io.vertx.proton.ProtonSession;

/**
 * A default {@code ReceiverFactory} for creating {@code ProtonReceiver} from a given connection.
 */
@Component
public class ReceiverFactoryImpl implements ReceiverFactory {

    private static final Logger LOG = LoggerFactory.getLogger(ReceiverFactoryImpl.class);

    @Override
    public void createReceiver(
            final ProtonConnection connection,
            final String address,
            final ProtonQoS qos,
            final Future<ProtonReceiver> result) {

        Objects.requireNonNull(connection);
        Objects.requireNonNull(address);
        Objects.requireNonNull(result);

        if (connection.isDisconnected()) {
            result.fail("connection is disconnected");
        } else {
            newSession(connection, remoteOpen -> {
                if (remoteOpen.succeeded()) {
                    newReceiver(connection, remoteOpen.result(), address, qos, result);
                } else {
                    result.fail(remoteOpen.cause());
                }
            });
        }
    }

    private void newSession(final ProtonConnection con, final Handler<AsyncResult<ProtonSession>> sessionOpenHandler) {
        con.createSession().openHandler(sessionOpenHandler).open();
    }

    private void newReceiver(
            final ProtonConnection connection,
            final ProtonSession session,
            final String address,
            final ProtonQoS qos,
            final Future<ProtonReceiver> result) {

        final ProtonReceiver receiver = session.createReceiver(address);
        receiver.setQoS(qos);
//        receiver.sendQueueDrainHandler(sendQueueDrainHandler);
        receiver.setAutoAccept(false).setPrefetch(0);
        receiver.openHandler(openAttempt -> {
            if (openAttempt.succeeded()) {
                LOG.debug(
                        "receiver [{}] for container [{}] open",
                        address,
                        connection.getRemoteContainer());
                result.complete(openAttempt.result());
            } else {
                LOG.debug("could not open receiver [{}] for container [{}]",
                        address,
                        connection.getRemoteContainer(), openAttempt.cause());
                result.fail(openAttempt.cause());
            }
        });
        receiver.closeHandler(closed -> {
            if (closed.succeeded()) {
                LOG.debug("receiver [{}] for container [{}] closed", address, connection.getRemoteContainer());
            }
        });
        receiver.open();
    }
}
