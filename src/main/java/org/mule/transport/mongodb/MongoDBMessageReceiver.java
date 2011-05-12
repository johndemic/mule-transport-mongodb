/*
 * $Id: MessageReceiver.vm 11079 2008-02-27 15:52:01Z tcarlson $
 * --------------------------------------------------------------------------------------
 * Copyright (c) MuleSource, Inc.  All rights reserved.  http://www.mulesource.com
 *
 * The software in this package is published under the terms of the CPAL v1.0
 * license, a copy of which has been included with this distribution in the
 * LICENSE.txt file.
 */

package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.util.JSON;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.api.MuleException;
import org.mule.api.construct.FlowConstruct;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.api.lifecycle.CreateException;
import org.mule.api.service.Service;
import org.mule.api.transport.Connector;
import org.mule.transport.AbstractPollingMessageReceiver;
import org.mule.transport.ConnectException;

import java.util.ArrayList;
import java.util.List;

/**
 * <code>MongoDBMessageReceiver</code> periodically polls a collection and returns the result.
 */
public class MongoDBMessageReceiver extends AbstractPollingMessageReceiver {


    public MongoDBMessageReceiver(Connector connector, FlowConstruct flowConstruct, InboundEndpoint endpoint)
            throws CreateException {
        super(connector, flowConstruct, endpoint);
    }


    public MongoDBMessageReceiver(Connector connector, Service service,
                                  InboundEndpoint endpoint)
            throws CreateException {

        super(connector, service, endpoint);
    }

    public void doConnect() throws ConnectException {
    }

    public void doDisconnect() throws ConnectException {
    }


    public void poll() throws Exception {

        String destination = endpoint.getEndpointURI().toString().split("://")[1];
        if (!destination.startsWith("bucket:")) {
            logger.debug("Polling collection: " + destination);
            pollCollection(destination);
        } else {
            logger.debug("Polling bucket: " + destination);
            pollBucket(destination.split("bucket:")[1]);
        }
    }

    void pollCollection(String collection) throws Exception {
        List<DBObject> result = new ArrayList<DBObject>();

        DBObject query = (DBObject) JSON.parse((String) endpoint.getProperty("query"));

        MongoDBConnector mongoConnector = (MongoDBConnector) connector;

        DB db = mongoConnector.getMongo().getDB(mongoConnector.getDatabase());

        db.requestStart();

        DBCursor cursor = db.getCollection(collection).find(query);

        while (cursor.hasNext()) {
            result.add(cursor.next());
        }

        db.requestDone();

        if (result.size() > 0) {
            routeMessage(createMuleMessage(result));
        }
    }

    void pollBucket(String bucket) throws Exception {

        MongoDBConnector mongoConnector = (MongoDBConnector) connector;

        DB db = mongoConnector.getMongo().getDB(mongoConnector.getDatabase());

        db.requestStart();

        GridFS gridFS = new GridFS(db, bucket);

        DBObject query = (DBObject) JSON.parse((String) endpoint.getProperty("query"));


        List<GridFSDBFile> results = gridFS.find(query);

        logger.debug(String.format("Query %s on bucket %s returned %d results", query, bucket, results.size()));

        db.requestDone();

        routeMessage(createMuleMessage(results));
    }


    @Override
    protected void doStart() throws MuleException {
        super.setFrequency(((MongoDBConnector) connector).pollingFrequency);
        super.doStart();
    }


}

