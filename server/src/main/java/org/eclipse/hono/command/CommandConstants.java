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

package org.eclipse.hono.command;

/**
 * Constants & utility methods used throughout the Command&amp;Control API.
 */
public final class CommandConstants {

    /**
     * The name of the command endpoint.
     */
    public static final String COMMAND_ENDPOINT = "command";
    /**
     * The name of the command reply endpoint.
     */
    public static final String COMMAND_REPLY_ENDPOINT = "command-reply";

    private CommandConstants() {
    }
}
