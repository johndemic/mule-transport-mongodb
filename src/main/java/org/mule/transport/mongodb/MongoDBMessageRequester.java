package org.mule.transport.mongodb;

import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.mule.DefaultMuleMessage;
import org.mule.api.MuleMessage;
import org.mule.api.endpoint.InboundEndpoint;
import org.mule.transport.AbstractMessageRequester;

import java.util.ArrayList;
import java.util.List;


public class MongoDBMessageRequester extends AbstractMessageRequester {

    public MongoDBMessageRequester(InboundEndpoint endpoint) {
        super(endpoint);
    }

    @Override
    protected MuleMessage doRequest(long timeout) throws Exception {
        String collection = endpoint.getName().split("endpoint.mongodb.")[1];
        List<DBObject> result = new ArrayList<DBObject>();

        logger.debug("Requesting all documents in collection: " + collection);
        DBCursor cursor = ((MongoDBConnector) connector).getDb().getCollection(collection).find();

        while (cursor.hasNext()) {
            result.add(cursor.next());
        }

        return new DefaultMuleMessage(result);
    }
}
