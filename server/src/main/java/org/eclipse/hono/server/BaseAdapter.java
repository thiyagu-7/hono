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
import java.util.UUID;

import org.eclipse.hono.config.HonoConfigProperties;
import org.eclipse.hono.util.MessageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.proton.ProtonClient;
import io.vertx.proton.ProtonClientOptions;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 *
 */
public abstract class BaseAdapter {

    protected Logger logger = LoggerFactory.getLogger(getClass());

    protected final Vertx vertx;
    protected HonoConfigProperties honoConfig = new HonoConfigProperties();
    protected String downstreamContainerHost;
    protected int downstreamContainerPort;
    protected ProtonConnection downstreamConnection;

    BaseAdapter(final Vertx vertx) {
        this.vertx = vertx;
    }

    /**
     * Sets the global Hono configuration properties.
     *
     * @param props The properties.
     * @throws NullPointerException if props is {@code null}.
     */
    @Autowired(required = false)
    public void setHonoConfiguration(final HonoConfigProperties props) {
        this.honoConfig = Objects.requireNonNull(props);
    }

    /**
     * Sets the name or IP address of the downstream AMQP 1.0 container to forward telemetry data to.
     *
     * @param host The hostname or IP address.
     * @throws NullPointerException if the host is {@code null}.
     */
    @Value("${hono.telemetry.downstream.host:localhost}")
    public final void setDownstreamContainerHost(final String host) {
        this.downstreamContainerHost = Objects.requireNonNull(host);
    }

    /**
     * Sets the port of the downstream AMQP 1.0 container to forward telemetry data to.
     *
     * @param port The port number.
     * @throws IllegalArgumentException if the given port is not a valid IP port.
     */
    @Value("${hono.telemetry.downstream.port:15672}")
    public final void setDownstreamContainerPort(final int port) {
        if (port < 1 || port >= 1 << 16) {
            throw new IllegalArgumentException("illegal port number");
        }
        this.downstreamContainerPort = port;
    }

    /**
     * Connects to the downstream container.
     *
     * @param startFuture The result of the connection attempt.
     * @throws IllegalStateException If the downstream container host is {@code null}
     *                               or the downstream container port is 0.
     */
    public final void start(final Future<Void> startFuture) {

        if (downstreamContainerHost == null) {
            throw new IllegalStateException("downstream container host is not set");
        } else if (downstreamContainerPort == 0) {
            throw new IllegalStateException("downstream container port is not set");
        } else {
            if (honoConfig.isWaitForDownstreamConnectionEnabled()) {
                logger.info("waiting for connection to downstream container");
                connectToDownstream(createClientOptions(), startFuture);
            } else {
                connectToDownstream(createClientOptions());
                startFuture.complete();
            }
        }
    }

    /**
     * Closes the connection with the downstream container.
     *
     * @param stopFuture Always succeeds.
     */
    public final void stop(final Future<Void> stopFuture) {

        if (downstreamConnection != null && !downstreamConnection.isDisconnected()) {
            final String container = downstreamConnection.getRemoteContainer();
            logger.info("closing connection to downstream container [{}]", container);
            downstreamConnection.closeHandler(null).disconnectHandler(null).close();
        } else {
            logger.debug("downstream connection already closed");
        }
        stopFuture.complete();
    }

    protected ProtonClientOptions createClientOptions() {
        return new ProtonClientOptions()
                .setConnectTimeout(100)
                .setReconnectAttempts(-1).setReconnectInterval(200); // reconnect forever, every 200 millisecs
    }

    private void connectToDownstream(final ProtonClientOptions options) {
        connectToDownstream(options, Future.future());
    }

    private void connectToDownstream(final ProtonClientOptions options, final Future<Void> connectFuture) {

        logger.info("connecting to downstream container [{}:{}]...", downstreamContainerHost, downstreamContainerPort);

        ProtonClient client = ProtonClient.create(vertx);
        client.connect(options, downstreamContainerHost, downstreamContainerPort, conAttempt -> {
            if (conAttempt.failed()) {
                logger.warn("can't connect to downstream AMQP 1.0 container [{}:{}]: {}", downstreamContainerHost, downstreamContainerPort, conAttempt.cause().getMessage());
            } else {
                logger.info("connected to downstream AMQP 1.0 container [{}:{}], opening connection ...",
                        downstreamContainerHost, downstreamContainerPort);
                final String containerName = "Hono-Adapter-" + UUID.randomUUID();
                conAttempt.result()
                        .setContainer(containerName)
                        .setHostname("hono-internal")
                        .openHandler(openCon -> {
                            if (openCon.succeeded()) {
                                downstreamConnection = openCon.result();
                                logger.info("connection to downstream container [{}] open",
                                        downstreamConnection.getRemoteContainer());
                                downstreamConnection.disconnectHandler(this::onDisconnectFromDownstreamContainer);
                                downstreamConnection.closeHandler(closedConnection -> {
                                    logger.info("connection to downstream container [{}] is closed", downstreamConnection.getRemoteContainer());
                                    downstreamConnection.close();
                                });
                                connectFuture.complete();
                            } else {
                                logger.warn("can't open connection to downstream container [{}]", containerName, openCon.cause());
                                connectFuture.fail(openCon.cause());
                            }
                        })
                        .closeHandler(closedCon -> logger.debug("Connection to [{}:{}] closed: {}", downstreamContainerHost, downstreamContainerPort))
                        .open();
            }
        });
    }

    /**
     * Handles unexpected disconnection from downstream container.
     *
     * @param con the failed connection
     */
    private void onDisconnectFromDownstreamContainer(final ProtonConnection con) {
        // all links to downstream host will now be stale and unusable
        logger.warn("lost connection to downstream container [{}]", downstreamContainerHost);
        onDisconnect();
        con.disconnectHandler(null);
        con.disconnect();
        ProtonClientOptions clientOptions = createClientOptions();
        if (clientOptions.getReconnectAttempts() != 0) {
            vertx.setTimer(300, reconnect -> {
                logger.info("attempting to re-connect to downstream container [{}]", downstreamContainerHost);
                connectToDownstream(clientOptions);
            });
        }
    }

    protected abstract void onDisconnect();

    public final void setDownstreamConnection(final ProtonConnection con) {
        this.downstreamConnection = con;
    }

}
