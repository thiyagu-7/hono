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

import org.apache.qpid.proton.amqp.transport.ErrorCondition;

import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 * Created by imm0420 on 29.11.2016.
 */
public interface ForwardingLink {

    /**
     * Closes the decorated link with an error condition.
     *
     * @param error The error condition to set on the link.
     */
    void close(ErrorCondition error);

    /**
     * Gets the link's unique identifier.
     *
     * @return The identifier.
     */
    String getLinkId();

    /**
     * Gets the ID of the client's connection to Hono that the decorated link is part of.
     * <p>
     * {@code HonoServer} assigns this (surrogate) identifier when a client establishes a connection with Hono.
     *
     * @return The ID.
     */
    String getConnectionId();

    boolean isOpen();

    /**
     * Gets the decorated link's target address.
     *
     * @return The address.
     */
    String getTargetAddress();


    /**
     * Gets the decorated link's source address.
     *
     * @return The address.
     */
    String getSourceAddress();


    void handleFlow(ProtonReceiver receiver);


    void handleFlow(ProtonSender sender);
}
