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
import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.transport.AbstractMessageDispatcher;
import org.mule.util.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.Date;
import java.util.List;
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

        MuleMessage responseMessage;

        if (!destination.startsWith("bucket:")) {
            logger.debug("Dispatching to collection: " + destination);
            result = doDispatchToCollection(event, destination);
            responseMessage = createMuleMessage(result);
        } else {
            result = doDispatchToBucket(event, destination.split("bucket:")[1]);
            responseMessage = createMuleMessage(result);
            GridFSInputFile file = (GridFSInputFile) result;
            if (StringUtils.isNotBlank(file.getFilename())) {
                responseMessage.setOutboundProperty("filename", file.getFilename());
            }
        }


        return responseMessage;
    }

    protected Object doDispatchToCollection(MuleEvent event, String collection) throws Exception {
        Object payload = event.getMessage().getPayload();

        DB db = connector.getMongo().getDB(connector.getDatabase());
        db.requestStart();

        if (payload instanceof List) {
            List list = (List) payload;
            for (Object o : list) {
                insertOrUpdate(getObject(o), db, collection);
            }
            db.requestDone();
            return payload;
        } else {
            BasicDBObject result = insertOrUpdate(getObject(payload), db, collection);
            db.requestDone();
            return result;
        }
    }

    protected BasicDBObject insertOrUpdate(BasicDBObject object, DB db, String collection) {
        if (object.containsField("_id")) {
            DBObject objectToUpdate = db.getCollection(collection).findOne(
                    new BasicDBObject("_id", new ObjectId(object.get("_id").toString())));


            if (objectToUpdate != null) {
                db.getCollection(collection).update(objectToUpdate, object);
            } else {
                logger.warn("Could not find existing object with _id: " + object.get("_id").toString() +
                        ", falling back to insert");
                db.getCollection(collection).insert(object);
            }
        } else {
            db.getCollection(collection).insert(object);
        }

        return object;
    }

    protected BasicDBObject getObject(Object payload) throws Exception {
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

        return object;
    }

    protected Object doDispatchToBucket(MuleEvent event, String bucket) throws Exception {
        DB db = connector.getMongo().getDB(connector.getDatabase());
        GridFS gridFS = new GridFS(db, bucket);

        db.requestStart();

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

        if (payload instanceof String) {
            file = gridFS.createFile(((String) payload).getBytes());

        }

        if (file == null) {
            throw new MongoDBException(String.format("Cannot persist objects of type %s to GridFS", payload.getClass()));
        }

        String filename = event.getMessage().getOutboundProperty(MongoDBConnector.PROPERTY_FILENAME, "");

        if (StringUtils.isNotBlank(filename)) {
            logger.debug("Setting filename on GridFS file to: " + filename);
            file.setFilename(filename);
        }

        logger.debug("Attempting to save file: " + file.getFilename());

        Date startTime = new Date();
        file.save();
        Date endTime = new Date();

        long elapsed = endTime.getTime() - startTime.getTime();

        logger.debug(String.format("GridFS file %s saved in %s seconds", file.getId(), elapsed / 1000.0));

        file.validate();

        db.requestDone();

        return file;
    }


}

