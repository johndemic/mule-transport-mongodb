/*
 * $Id$
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb;

import org.mule.transport.AbstractMessageDispatcherFactory;
import org.mule.api.MuleException;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.api.transport.MessageDispatcher;

/**
 * <code>MongoDBMessageDispatcherFactory</code> creates <code>MongoDBMessageDispatchers</code>
 */

public class MongoDBMessageDispatcherFactory extends AbstractMessageDispatcherFactory
{


    public MessageDispatcher create(OutboundEndpoint endpoint) throws MuleException
    {
        return new MongoDBMessageDispatcher(endpoint);
    }

}
