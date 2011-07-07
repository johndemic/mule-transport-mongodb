package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import org.codehaus.jackson.map.ObjectMapper;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;
import org.mule.util.StringUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoDBMessageRequester extends AbstractMessageRequester {

    ObjectMapper mapper;


    public MongoDBMessageRequester(InboundEndpoint endpoint) {
        super(endpoint);
        mapper = new ObjectMapper();
    }

    @Override
    protected MuleMessage doRequest(long timeout) throws Exception {

        String query = (String) endpoint.getEndpointURI().getParams().get("query");
        logger.debug("Query string: " + query);

        String destination = endpoint.getEndpointURI().toString().split("://")[1].split("\\?")[0];

        if (!destination.startsWith("bucket:")) {
            logger.debug("Requesting  collection: " + destination);
            return createMuleMessage(doRequestForCollection(destination, query));
        } else {
            logger.debug("Requesting bucket: " + destination);
            return createMuleMessage(doRequestForBucket(destination.split("bucket:")[1], query));
        }
    }

    Object doRequestForCollection(String collection, String queryString) throws Exception {

        List<Map> result = new ArrayList<Map>();

        logger.debug("Requesting all documents in collection: " + collection);

        MongoDBConnector mongoConnector = (MongoDBConnector) connector;
        DB db = mongoConnector.getMongo().getDB(mongoConnector.getMongoURI().getDatabase());

        DBCursor cursor;

        db.requestStart();

        if (StringUtils.isNotBlank(queryString)) {
            BasicDBObject query = MongoUtils.scrubQueryObjectId(mapper.readValue(queryString, BasicDBObject.class));
            cursor = db.getCollection(collection).find(query);
        } else {
            cursor = db.getCollection(collection).find();
        }

        while (cursor.hasNext()) {
            result.add(cursor.next().toMap());
        }

        db.requestDone();


        return result;
    }

    Object doRequestForBucket(String bucket, String queryString) throws Exception {

        MongoDBConnector mongoConnector = (MongoDBConnector) connector;
        DB db = mongoConnector.getMongo().getDB(mongoConnector.getMongoURI().getDatabase());

        GridFS gridFS = new GridFS(db, bucket);

        if (StringUtils.isBlank(queryString))
            queryString = "{}";

        BasicDBObject query;

        try {
            query = MongoUtils.scrubQueryObjectId(mapper.readValue(queryString, BasicDBObject.class));
        } catch (IOException e) {
            throw new MongoDBException(e);
        }

        List<GridFSDBFile> results = gridFS.find(query);

        logger.debug(String.format("Query %s on bucket %s returned %d results", query, bucket, results.size()));
        return results;
    }


}
