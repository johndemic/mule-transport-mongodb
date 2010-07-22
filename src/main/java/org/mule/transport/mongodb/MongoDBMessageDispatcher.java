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
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.transport.AbstractMessageDispatcher;
import org.mule.util.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

/**
 * <code>MongoDBMessageDispatcher</code> inserts records into a MongoDB collection specified on the endpoint
 */
public class MongoDBMessageDispatcher extends AbstractMessageDispatcher {

    MongoDBConnector connector;

    ObjectMapper mapper;

    public MongoDBMessageDispatcher(OutboundEndpoint endpoint) {
        super(endpoint);
        connector = (MongoDBConnector) endpoint.getConnector();
        mapper = new ObjectMapper();
    }

    public void doConnect() throws Exception {
    }

    public void doDisconnect() throws Exception {

    }


    public void doDispatch(MuleEvent event) throws Exception {


        logger.debug("Attempting to evaluate endpoint: " + event.getEndpoint().getEndpointURI().toString());
        String evaluatedEndpoint =
                event.getMuleContext().getExpressionManager().parse(event.getEndpoint().getEndpointURI().toString(),
                        event.getMessage());
        logger.debug("Evaluated endpoint: " + evaluatedEndpoint);

        String destination = evaluatedEndpoint.split("://")[1];

        event.transformMessage();

        if (!destination.startsWith("bucket:")) {
            logger.debug("Dispatching to collection: " + destination);
            doDispatchToCollection(event, destination);
        } else {
            logger.debug("Dispatching to bucket: " + destination);
            doDispatchToBucket(event, destination.split("bucket:")[1]);
        }

    }

    public MuleMessage doSend(MuleEvent event) throws Exception {

        logger.debug("Attempting to evaluate endpoint: " + event.getEndpoint().getEndpointURI().toString());
        String evaluatedEndpoint =
                event.getMuleContext().getExpressionManager().parse(event.getEndpoint().getEndpointURI().toString(),
                        event.getMessage());

        logger.debug("Evaluated endpoint: " + evaluatedEndpoint);

        String destination = evaluatedEndpoint.split("://")[1];

        Object result;

        event.transformMessage();

        if (!destination.startsWith("bucket:")) {
            logger.debug("Dispatching to collection: " + destination);
            result = doDispatchToCollection(event, destination);
        } else {
            result = doDispatchToBucket(event, destination.split("bucket:")[1]);
        }

        return new DefaultMuleMessage(result);
    }

    protected Object doDispatchToCollection(MuleEvent event, String collection) throws Exception {
        Object payload = event.getMessage().getPayload();

        BasicDBObject object = null;


        if (payload instanceof String) {
            object = mapper.readValue((String) payload, BasicDBObject.class);
        }

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

        return object;
    }

    protected Object doDispatchToBucket(MuleEvent event, String bucket) throws Exception {
        GridFS gridFS = new GridFS(connector.getDb(), bucket);

        Object payload = event.getMessage().getPayload();

        GridFSInputFile file = null;

        if (payload instanceof File) {
            file = gridFS.createFile((File) payload);
        }

        if (payload instanceof InputStream) {
            file = gridFS.createFile((InputStream) payload);
        }

        if (payload instanceof byte[]) {
            file = gridFS.createFile((byte[]) payload);
        }

        if (file == null) {
            throw new MongoDBException("Cannot persist objects of type to GridFS: " + payload.getClass());
        }

        String filename = event.getMessage().getStringProperty(MongoDBConnector.PROPERTY_FILENAME,"");

        if (StringUtils.isNotBlank(filename)) {
            logger.debug("Setting filename on GridFS file to: " + filename);
            file.setFilename(filename);
        }       

        logger.debug("Attempting to save file: " + file.getFilename());
        file.save();
        return file;
    }

}

