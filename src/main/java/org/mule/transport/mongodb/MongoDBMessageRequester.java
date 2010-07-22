package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class MongoDBMessageRequester extends AbstractMessageRequester {

    ObjectMapper mapper;


    public MongoDBMessageRequester(InboundEndpoint endpoint) {
        super(endpoint);
        mapper = new ObjectMapper();

    }

    @Override
    protected MuleMessage doRequest(long timeout) throws Exception {

        String destination = endpoint.getEndpointURI().toString().split("://")[1];
        if (!destination.startsWith("bucket:")) {
            logger.debug("Requesting  collection: " + destination);
            return new DefaultMuleMessage(doRequestForCollection(destination));
        } else {
            logger.debug("Requesting bucket: " + destination);
            return new DefaultMuleMessage(doRequestForBucket(destination.split("bucket:")[1]));
        }
    }


    Object doRequestForCollection(String collection) {

        List<DBObject> result = new ArrayList<DBObject>();

        logger.debug("Requesting all documents in collection: " + collection);
        DBCursor cursor = ((MongoDBConnector) connector).getDb().getCollection(collection).find();

        while (cursor.hasNext()) {
            result.add(cursor.next());
        }

        return result;
    }

    Object doRequestForBucket(String bucket) {
        GridFS gridFS = new GridFS(((MongoDBConnector) connector).getDb(), bucket);
        BasicDBObject query = null;
        try {
            query = mapper.readValue("{}", BasicDBObject.class);
        } catch (IOException e) {
            throw new MongoDBException(e);
        }

        List<GridFSDBFile> results = gridFS.find(query);

        logger.debug(String.format("Query %s on bucket %s returned %d results", query, bucket, results.size()));
        return results;

    }
}
