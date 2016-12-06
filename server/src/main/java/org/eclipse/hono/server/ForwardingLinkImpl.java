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

import org.apache.qpid.proton.amqp.transport.ErrorCondition;
import org.eclipse.hono.util.Constants;
import org.eclipse.hono.util.MessageHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.vertx.proton.ProtonLink;
import io.vertx.proton.ProtonQoS;
import io.vertx.proton.ProtonReceiver;
import io.vertx.proton.ProtonSender;

/**
 *
 */
public class ForwardingLinkImpl implements ForwardingLink, UpstreamReceiver, UpstreamSender {

    private static final Logger LOG = LoggerFactory.getLogger(ForwardingLinkImpl.class);
    private final ProtonLink link;
    private final String id;

    private ForwardingLinkImpl(final String linkId, final ProtonReceiver receiver, final ProtonQoS qos) {
        this.id = Objects.requireNonNull(linkId);
        this.link = Objects.requireNonNull(receiver);
        receiver.setAutoAccept(false).setPrefetch(0).setQoS(qos);
    }

    private ForwardingLinkImpl(final String linkId, final ProtonSender sender, final ProtonQoS qos) {
        this.id = Objects.requireNonNull(linkId);
        this.link = Objects.requireNonNull(sender);
        sender.setQoS(qos).setAutoDrained(true);
    }

    /**
     * Creates a new instance for an identifier and a receiver link.
     * <p>
     * The receiver is configured for manual flow control and disposition handling.
     *
     * @param linkId The identifier for the link.
     * @param receiver The link for receiving data from the client.
     * @param qos The quality of service the receiver should use.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamReceiver newUpstreamReceiver(final String linkId, final ProtonReceiver receiver, final ProtonQoS qos) {
        return new ForwardingLinkImpl(linkId, receiver, qos);
    }

    /**
     * Creates a new instance for an identifier and a receiver link.
     * <p>
     * The receiver is configured for manual flow control and disposition handling
     * and uses <em>AT_MOST_ONCE</em> quality of service.
     *
     * @param linkId The identifier for the link.
     * @param receiver The link for receiving data from the client.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamReceiver atMostOnceReceiver(final String linkId, final ProtonReceiver receiver) {
        return new ForwardingLinkImpl(linkId, receiver, ProtonQoS.AT_MOST_ONCE);
    }

    /**
     * Creates a new instance for an identifier and a receiver link.
     * <p>
     * The receiver is configured for manual flow control and disposition handling.
     * and uses <em>AT_LEAST_ONCE</em> quality of service.
     *
     * @param linkId The identifier for the link.
     * @param receiver The link for receiving data from the client.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamReceiver atLeastOnceReceiver(final String linkId, final ProtonReceiver receiver) {
        return new ForwardingLinkImpl(linkId, receiver, ProtonQoS.AT_LEAST_ONCE);
    }

    /**
     * Creates a new instance for an identifier and a sender link.
     * <p>
     * The sender is configured for manual flow control and disposition handling.
     *
     * @param linkId The identifier for the link.
     * @param sender The link for sending data to the client.
     * @param qos The quality of service the sender should use.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamSender newUpstreamSender(final String linkId, final ProtonSender sender, final ProtonQoS qos) {
        return new ForwardingLinkImpl(linkId, sender, qos);
    }

    /**
     * Creates a new instance for an identifier and a sender link.
     * <p>
     * The receiver is configured for manual flow control and disposition handling
     * and uses <em>AT_MOST_ONCE</em> quality of service.
     *
     * @param linkId The identifier for the link.
     * @param sender The link for sending data to the client.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamSender atMostOnceSender(final String linkId, final ProtonSender sender) {
        return new ForwardingLinkImpl(linkId, sender, ProtonQoS.AT_MOST_ONCE);
    }

    /**
     * Creates a new instance for an identifier and a sender link.
     * <p>
     * The receiver is configured for manual flow control and disposition handling.
     * and uses <em>AT_LEAST_ONCE</em> quality of service.
     *
     * @param linkId The identifier for the link.
     * @param sender The link for sending data to the client.
     * @return The created instance.
     * @throws NullPointerException if any of the parameters is {@code null}.
     */
    public static UpstreamSender atLeastOnceSender(final String linkId, final ProtonSender sender) {
        return new ForwardingLinkImpl(linkId, sender, ProtonQoS.AT_LEAST_ONCE);
    }

    @Override
    public String getLinkId() {
        return id;
    }

    @Override
    public String getConnectionId() {
        return Constants.getConnectionId(link);
    }

    @Override
    public String getTargetAddress() {
        return link.getRemoteTarget().getAddress();
    }

    @Override
    public String getSourceAddress() {
        return link.getRemoteSource().getAddress();
    }

    @Override
    public void close(final ErrorCondition error) {
        if (error != null) {
            link.setCondition(error);
        }
        link.close();
    }

    @Override
    public boolean isOpen() {
        return link.isOpen();
    }

    private void handleFlow(final ProtonReceiver receiver, final ProtonSender sender) {
        final int credits = getAvailableCredit(sender);
        LOG.trace("received FLOW from sender for upstream client [con:{}, link: {}, credits: {}, drain: {}]",
        sender.getSession(), MessageHelper.getLinkName(sender), credits, sender.getDrain());
        if (sender.getDrain()) {
            // send drain request upstream and act upon result of request to drain upstream client
            receiver.drain(10000, drainAttempt -> {
                if (drainAttempt.succeeded()) {
                    sender.drained();
                }
            });
        } else {
            LOG.debug("replenishing client [{}] with {} credits", receiver.getRemoteSource(), credits);
            receiver.flow(credits);
        }
    }

    private int getAvailableCredit(final ProtonSender sender) {
        // TODO: is it correct to subtract the queued messages?
        return sender.getCredit() - sender.getQueued();
    }

    @Override
    public void handleFlow(final ProtonReceiver receiver){
        if (link instanceof ProtonSender) {
            final ProtonSender sender = (ProtonSender) this.link;
            sender.sendQueueDrainHandler(replenishedSender -> handleFlow(receiver, replenishedSender));
        } else {
            throw new UnsupportedOperationException("Cannot connect two receivers.");
        }
    }

    @Override
    public void handleFlow(final ProtonSender sender){
        if (link instanceof ProtonReceiver) {
            sender.sendQueueDrainHandler(replenishedSender -> handleFlow((ProtonReceiver) link, replenishedSender));
        } else {
            throw new UnsupportedOperationException("Cannot connect two senders.");
        }
    }

    @Override
    public ProtonSender getSender() {
        if (link instanceof ProtonSender) {
            return (ProtonSender) link;
        } else {
            throw new UnsupportedOperationException("This link is a receiver.");
        }
    }

    @Override
    public ProtonReceiver getReceiver() {
        if (link instanceof ProtonReceiver) {
            return (ProtonReceiver) link;
        } else {
            throw new UnsupportedOperationException("This link is a sender.");
        }
    }

    @Override
    public void replenish(final int replenishedCredits) {
        getReceiver().flow(replenishedCredits);
    }

    @Override
    public String toString() {
        boolean isSender = link instanceof ProtonSender;
        final StringBuffer sb = new StringBuffer("ForwardingLink{");
        sb.append("id='").append(id).append('\'');
        sb.append(", connection='").append(getConnectionId()).append('\'');
        sb.append(", ")
                .append(isSender ? "sender" : "receiver")
                .append("='")
                .append(MessageHelper.getLinkName(link)).append('\'');
        sb.append(", credit='").append(link.getCredit()).append('\'');
        sb.append(", queued='").append(link.getQueued()).append('\'');
        sb.append(isSender ? "target='" : "source='").append(isSender ? link.getRemoteTarget() : link.getRemoteSource()).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
