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
import org.apache.qpid.proton.message.Message;

import io.vertx.core.Handler;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 * A decorator for a {@code ProtonSender} representing a client receiving data from a Hono endpoint.
 *
 */
public interface UpstreamSender extends ForwardingLink {

    ProtonSender getSender();

//    /**
//     * Creates a new instance for an identifier and a sender link.
//     * <p>
//     * The sender is configured for manual flow control and disposition handling.
//     *
//     * @param linkId The identifier for the link.
//     * @param sender The link for sending data to the client.
//     * @param qos The quality of service the sender should use.
//     * @return The created instance.
//     * @throws NullPointerException if any of the parameters is {@code null}.
//     */
//    static UpstreamSender newUpstreamSender(final String linkId, final ProtonSender sender, final ProtonQoS qos) {
//        return new UpstreamSenderImpl(linkId, sender, qos);
//    }
//
//    /**
//     * Creates a new instance for an identifier and a receiver link.
//     * <p>
//     * The receiver is configured for manual flow control and disposition handling
//     * and uses <em>AT_MOST_ONCE</em> quality of service.
//     *
//     * @param linkId The identifier for the link.
//     * @param receiver The link for receiving data from the client.
//     * @return The created instance.
//     * @throws NullPointerException if any of the parameters is {@code null}.
//     */
////    static UpstreamSender atMostOnceReceiver(final String linkId, final ProtonReceiver receiver) {
////        return new UpstreamReceiverImpl(linkId, receiver, ProtonQoS.AT_MOST_ONCE);
////    }
//
//    /**
//     * Creates a new instance for an identifier and a receiver link.
//     * <p>
//     * The receiver is configured for manual flow control and disposition handling.
//     * and uses <em>AT_LEAST_ONCE</em> quality of service.
//     *
//     * @param linkId The identifier for the link.
//     * @param receiver The link for receiving data from the client.
//     * @return The created instance.
//     * @throws NullPointerException if any of the parameters is {@code null}.
//     */
////    static UpstreamSender atLeastOnceReceiver(final String linkId, final ProtonReceiver receiver) {
////        return new UpstreamReceiverImpl(linkId, receiver, ProtonQoS.AT_LEAST_ONCE);
////    }
//
//
//    void forwardMessage(ProtonDelivery delivery, Message message);
//
//
//    /**
//     * Gets the decorated link's source address.
//     *
//     * @return The address.
//     */
//    String getSourceAddress();
//
//
//    void handleFlow(ProtonReceiver receiver);
}