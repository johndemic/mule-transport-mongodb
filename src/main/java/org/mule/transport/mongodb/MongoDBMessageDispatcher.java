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

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.transport.AbstractMessageDispatcher;

import java.util.Map;

/**
 * <code>MongoDBMessageDispatcher</code> inserts records into a MongoDB collection specified on the endpoint
 */
public class MongoDBMessageDispatcher extends AbstractMessageDispatcher {

    MongoDBConnector connector;

    public MongoDBMessageDispatcher(OutboundEndpoint endpoint) {
        super(endpoint);
        connector = (MongoDBConnector) endpoint.getConnector();
    }

    public void doConnect() throws Exception {
    }

    public void doDisconnect() throws Exception {

    }

    public void doDispatch(MuleEvent event) throws Exception {
        String collection =
                event.getMuleContext().getExpressionManager().parse(event.getEndpoint().getName(), event.getMessage())
                        .split("\\.")[2];

        event.transformMessage();
        
        Object payload = event.getMessage().getPayload();

        BasicDBObject object = null;

        if (payload instanceof Map) {
            object = new BasicDBObject((Map) payload);
        }

        if (payload instanceof BasicDBObject) {
            object = (BasicDBObject) payload;
        }

        if (object == null) {
            throw new MongoDBException("Cannot persist objects of type: " + payload.getClass());
        }

        synchronized (this) {
            connector.getDb().getCollection(collection).insert(object);
        }
    }

    public MuleMessage doSend(MuleEvent event) throws Exception {
        doDispatch(event);
        return new DefaultMuleMessage(event.getMessage().getPayload());
    }

}

