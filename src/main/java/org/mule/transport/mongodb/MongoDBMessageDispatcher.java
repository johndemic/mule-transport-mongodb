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

import com.mongodb.*;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSInputFile;
import com.mongodb.util.JSON;
import org.bson.types.ObjectId;
import org.mule.api.MuleEvent;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.OutboundEndpoint;
import org.mule.transport.AbstractMessageDispatcher;
import org.mule.util.StringUtils;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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

            if (!endpoint.getProperties().containsKey("query")) {
                result = doDispatchToCollection(event, destination);
            } else {
                result = doQueryCollection(event, destination);
            }
            responseMessage = createMuleMessage(result);
        } else {
            result = doDispatchToBucket(event, destination.split("bucket:")[1]);
            responseMessage = createMuleMessage(result);
            GridFSInputFile file = (GridFSInputFile) result;
            if (StringUtils.isNotBlank(file.getFilename())) {
                responseMessage.setOutboundProperty("filename", file.getFilename());
            }
        }

        if (event.getMessage().getOutboundPropertyNames().contains(MongoDBConnector.PROPERTY_OBJECT_ID)) {
            responseMessage.setOutboundProperty(MongoDBConnector.PROPERTY_OBJECT_ID,
                    event.getMessage().<Object>getOutboundProperty(MongoDBConnector.PROPERTY_OBJECT_ID));
        }
        return responseMessage;
    }

    protected Object doQueryCollection(MuleEvent event, String collection) throws Exception {

        List<DBObject> result = new ArrayList<DBObject>();

        String queryString =
                event.getMuleContext().getExpressionManager().parse((String) endpoint.getProperty("query"),
                        event.getMessage());

        logger.debug("Evaluated query: " + queryString);
        DBObject query = (DBObject) JSON.parse(queryString);

        DB db = connector.getMongo().getDB(connector.getDatabase());

        db.requestStart();

        DBCursor cursor = db.getCollection(collection).find(query);

        while (cursor.hasNext()) {
            result.add(cursor.next());
        }

        db.requestDone();
        return result;
    }

    protected Object doDispatchToCollection(MuleEvent event, String collection) throws Exception {
        Object payload = event.getMessage().getPayload();

        DB db = connector.getMongo().getDB(connector.getDatabase());
        db.requestStart();

        if (payload instanceof List) {
            List list = (List) payload;

            for (Object o : list) {
                performOperation(event, o, db, collection);
            }

            db.requestDone();
            return payload;
        } else {
            MuleMessage message = event.getMessage();

            BasicDBObject result = performOperation(event, message.getPayload(), db, collection);

            Object id = getObjectIdAsString(result.get("_id"));

            if (id == null) {
                logger.warn("_id is null, cannot set " + MongoDBConnector.PROPERTY_OBJECT_ID);
            } else {
                event.getMessage().setOutboundProperty(MongoDBConnector.PROPERTY_OBJECT_ID, id);
            }
            db.requestDone();

            return result;
        }
    }

    protected String getObjectIdAsString(Object objectId) {

        if (objectId == null) return null;

        if (objectId instanceof String) {
            return (String) objectId;
        } else if (objectId instanceof ObjectId) {
            return ((ObjectId) objectId).toStringMongod();
        } else {
            return null;
        }
    }

    protected BasicDBObject performOperation(MuleEvent event, Object object, DB db, String collection) throws Exception {

        BasicDBObject result;

        MuleMessage message = event.getMessage();

        if (message.getOutboundPropertyNames().contains(MongoDBConnector.MULE_MONGO_DISPATCH_MODE) ||
                event.getEndpoint().getProperties().containsKey("dispatchMode")) {
            String mode = StringUtils.upperCase((String)
                    message.getOutboundProperty(MongoDBConnector.MULE_MONGO_DISPATCH_MODE, null));
            if (mode == null) {
                mode = StringUtils.upperCase((String) event.getEndpoint().getProperty("dispatchMode"));
            }

            logger.debug("dispatch mode is set to: " + mode);

            switch (MongoDBDispatchMode.valueOf(mode)) {
                case INSERT:
                    result = insert(getObject(object), db, collection, event);
                    break;
                case UPDATE:
                    result = update(getObject(object), db, collection, event);
                    break;
                case DELETE:
                    result = delete(getObject(object), db, collection, event);
                    break;
                default:
                    throw new MongoDBException("No dispatch mode associated with: " + mode);
            }
        } else {
            result = insert(getObject(object), db, collection, event);
        }

        return result;
    }

    protected BasicDBObject insert(BasicDBObject object, DB db, String collection, MuleEvent event) {
        logger.debug(String.format("Inserting to collection %s in DB %s: %s", collection, db, object));
        WriteConcern writeConcern = WriteConcernFactory.getWriteConcern(event);
        if (writeConcern == null) {
            db.getCollection(collection).insert(object);
        } else {
            logger.debug("Using WriteConcern value " + writeConcern);
            db.getCollection(collection).insert(object, writeConcern);
        }
        return object;
    }

    protected BasicDBObject update(BasicDBObject object, DB db, String collection, MuleEvent event) throws Exception {
        logger.debug(String.format("Updating collection %s in DB %s: %s", collection, db, object));

        boolean upsert = false;
        boolean multi = false;

        MuleMessage message = event.getMessage();

        if (message.getOutboundPropertyNames().contains(MongoDBConnector.MULE_MONGO_UPDATE_UPSERT)) {
            if (message.getOutboundProperty(MongoDBConnector.MULE_MONGO_UPDATE_UPSERT).equals("true"))
                upsert = true;
        }

        if (event.getEndpoint().getProperties().containsKey("upsert")) {
            if (event.getEndpoint().getProperty("upsert").equals("true"))
                upsert = true;
        }

        if (message.getOutboundPropertyNames().contains(MongoDBConnector.MULE_MONGO_UPDATE_MULTI)) {
            if (message.getOutboundProperty(MongoDBConnector.MULE_MONGO_UPDATE_MULTI).equals("true"))
                multi = true;
        }

        if (event.getEndpoint().getProperties().containsKey("multi")) {
            if (event.getEndpoint().getProperty("multi").equals("true"))
                multi = true;
        }

        DBObject objectToUpdate;

        if (!message.getOutboundPropertyNames().contains(MongoDBConnector.MULE_MONGO_UPDATE_QUERY) &&
                !event.getEndpoint().getProperties().containsKey("updateQuery")) {
            logger.debug(String.format("%s property is  not set, updating object using _id value of payload",
                    MongoDBConnector.MULE_MONGO_UPDATE_QUERY));
            objectToUpdate = db.getCollection(collection).findOne(
                    new BasicDBObject("_id", new ObjectId(object.get("_id").toString())));
            if (!upsert) {
                throw new MongoException("Object not found from update query and upsert is false");
            }
        } else {
            String updateQuery = message.getOutboundProperty(MongoDBConnector.MULE_MONGO_UPDATE_QUERY);

            if (updateQuery == null) {
                updateQuery = (String) event.getEndpoint().getProperty("updateQuery");
            }

            String evaluatedQuery = message.getMuleContext().getExpressionManager().parse(updateQuery, message);

            logger.debug(String.format("%s property is set, building query from %s",
                    MongoDBConnector.MULE_MONGO_UPDATE_QUERY, evaluatedQuery));

            objectToUpdate = (DBObject) JSON.parse(evaluatedQuery);

            if (objectToUpdate == null)
                throw new MongoException("Could not find create update query from: " + updateQuery);
        }

        WriteConcern writeConcern = WriteConcernFactory.getWriteConcern(event);

        if (writeConcern == null) {
            db.getCollection(collection).update(objectToUpdate, object, upsert, multi);
        } else {
            logger.debug("Using WriteConcern value " + writeConcern);
            db.getCollection(collection).update(objectToUpdate, object, upsert, multi, writeConcern);
        }

        return object;
    }

    protected BasicDBObject delete(BasicDBObject object, DB db, String collection, MuleEvent event) {

        logger.debug(String.format("Deleting from collection %s in DB %s: %s", collection, db, object));


        WriteConcern writeConcern = WriteConcernFactory.getWriteConcern(event);

        if (writeConcern == null) {
            db.getCollection(collection).remove(object);
        } else {
            logger.debug("Using WriteConcern value " + writeConcern);
            db.getCollection(collection).remove(object, writeConcern);
        }
        return object;
    }


    protected BasicDBObject getObject(Object payload) throws Exception {
        BasicDBObject object = null;

        if (payload instanceof String) {

            object = (BasicDBObject) JSON.parse((String ) payload);
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

        ObjectId id = (ObjectId) file.getId();
        event.getMessage().setOutboundProperty(MongoDBConnector.PROPERTY_OBJECT_ID, id.toStringMongod());

        db.requestDone();

        return file;
    }

}

