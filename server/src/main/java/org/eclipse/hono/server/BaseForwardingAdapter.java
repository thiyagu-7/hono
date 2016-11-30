/**
 * Copyright (c) 2016 Bosch Software Innovations GmbH. All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0 which accompanies this distribution, and is
 * available at http://www.eclipse.org/legal/epl-v10.html Contributors: Bosch Software Innovations GmbH - initial
 * creation
 */
package org.eclipse.hono.server;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.eclipse.hono.util.Constants;

import io.vertx.core.Vertx;
import io.vertx.proton.ProtonLink;

/**
 * Handles link/connection management for upstream/downstream forwarding adapters.
 */
public abstract class BaseForwardingAdapter<T extends ProtonLink> extends BaseAdapter
{
    private final Map<String, T> activeLinks = new HashMap<>();
    private final Map<String, List<String>> linksPerConnection = new HashMap<>();

    BaseForwardingAdapter(final Vertx vertx) {
        super(vertx);
    }

    public final void addLink(final String connectionId, final String linkId, final T link) {
        activeLinks.put(linkId, link);
        final List<String> links = linksPerConnection.computeIfAbsent(connectionId, k -> new ArrayList<>());
        links.add(linkId);
    }

    protected final T removeLink(final String linkId) {
        return activeLinks.remove(linkId);
    }

    protected final boolean containsLink(final String linkId) {
        return activeLinks.containsKey(linkId);
    }
    protected final T getLink(final String linkId) {
        return activeLinks.get(linkId);
    }

    protected final void removeLinkForConnection(final String connectionId, final String linkId) {
        if (connectionId != null) {
            final List<String> senders = linksPerConnection.get(connectionId);
            if (senders != null) {
                senders.remove(linkId);
            }
        }
    }

    protected final List<String> removeLinksForConnection(final String connectionId) {
        return linksPerConnection.remove(connectionId);
    }

    protected final void clearLinks() {
        activeLinks.clear();
    }

    public void onClientDetach(final ForwardingLink client) {
        final String connectionId = closeLink(client.getLinkId());
        removeLinkForConnection(connectionId, client.getLinkId());
    }

    public void onClientDetach(final UpstreamReceiver client) {
        onClientDetach((ForwardingLink) client);
    }

    public void onClientDetach(final UpstreamSender client) {
        onClientDetach((ForwardingLink) client);
    }

    private String closeLink(final String linkId) {
        final ProtonLink link = removeLink(linkId);
        if (link != null && link.isOpen()) {
            final String connectionId = Constants.getConnectionId(link);
            logger.info("closing downstream link [con: {}, link: {}]", connectionId, linkId);
            link.close();
            return connectionId;
        } else {
            return null;
        }
    }

    public void onClientDisconnect(final String connectionId) {
        final List<String> senders = removeLinksForConnection(Objects.requireNonNull(connectionId));
        if (senders != null && !senders.isEmpty()) {
            logger.info("closing {} downstream senders for connection [id: {}]", senders.size(), connectionId);
            for (final String linkId : senders) {
                closeLink(linkId);
            }
        }
    }

    @Override
    protected void onDisconnect() {
        clearLinks();
    }
}
