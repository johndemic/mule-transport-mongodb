/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb.i18n;

import org.mule.config.i18n.Message;
import org.mule.config.i18n.MessageFactory;

public class MongodbMessages extends MessageFactory {
    private static final String BUNDLE_PATH = getBundlePath("mongodb");

    private static final MongodbMessages factory = new MongodbMessages();


    public static Message gridfsFileNoConnectorRef() {
        return factory.createMessage(BUNDLE_PATH, 1);
    }

}
