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
import org.eclipse.hono.server.ForwardingDownstreamAdapter;
import org.eclipse.hono.server.ForwardingUpstreamAdapter;
import org.eclipse.hono.server.ReceiverFactory;
import org.eclipse.hono.server.SenderFactory;
import org.eclipse.hono.server.UpstreamSender;
import org.eclipse.hono.util.MessageHelper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonConnection;
import io.vertx.proton.ProtonDelivery;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonSender;

/**
 *
 */
@Component
@Scope("prototype")
@Qualifier("command")
public class ForwardingCommandUpstreamAdapter extends ForwardingUpstreamAdapter {

    @Autowired
    public ForwardingCommandUpstreamAdapter(final Vertx vertx, final ReceiverFactory receiverFactory) {
        super(vertx, receiverFactory);
    }

    @Override
    protected void forwardMessage(final ProtonSender sender, final Message msg, final ProtonDelivery delivery) {
        logger.debug("forwarding command message [id: {}, to: {}, content-type: {}] to upstream client [{}]",
                msg.getMessageId(), msg.getAddress(), msg.getContentType(), MessageHelper.getLinkName(sender));
        sender.send(msg, updatedDelivery -> delivery.disposition(updatedDelivery.getRemoteState(), updatedDelivery.remotelySettled()));
    }

    @Override protected ProtonQoS getDownstreamQos() {
        return ProtonQoS.AT_LEAST_ONCE;
    }
}
