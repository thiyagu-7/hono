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

import static io.vertx.proton.ProtonHelper.condition;

import org.apache.qpid.proton.message.Message;
import org.eclipse.hono.command.CommandConstants;
import org.eclipse.hono.command.CommandMessageFilter;
import org.eclipse.hono.event.EventMessageFilter;
import org.eclipse.hono.server.DownstreamAdapter;
import org.eclipse.hono.server.MessageForwardingEndpoint;
import org.eclipse.hono.server.UpstreamAdapter;
import org.eclipse.hono.util.ResourceIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonQoS;

/**
 *
 */
@Component
@Scope("prototype")
@Qualifier("command")
public class CommandEndpoint extends MessageForwardingEndpoint {

    public CommandEndpoint(final Vertx vertx) {
        super(vertx);
    }

    @Override public String getName() {
        return CommandConstants.COMMAND_ENDPOINT;
    }

    @Override protected ProtonQoS getEndpointQos() {
        return ProtonQoS.AT_LEAST_ONCE;
    }

    @Override
    protected boolean passesFormalVerification(final ResourceIdentifier targetAddress, final Message message) {
        return CommandMessageFilter.verify(targetAddress, message);
    }

    @Autowired
    @Qualifier("command")
    public final void setCommandAdapter(final DownstreamAdapter adapter) {
        setDownstreamAdapter(adapter);
    }

    @Autowired
    @Qualifier("command")
    public final void setCommandAdapter(final UpstreamAdapter adapter) {
        setUpstreamAdapter(adapter);
    }
}
