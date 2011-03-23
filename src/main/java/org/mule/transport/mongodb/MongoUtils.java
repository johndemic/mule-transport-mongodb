package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;

public class MongoUtils {

    /**
     * Replaces String instances of _id in a query with an instance of <code>ObjectId</code>.
     * @param query the query to scrub
     * @return the scrubbed query.
     */
    public static BasicDBObject scrubQueryObjectId(BasicDBObject query) {

        if (query.containsField("_id")) {
            ObjectId id = new ObjectId(query.get("_id").toString());
            query.removeField("_id");
            query.put("_id", id);
        }
        return query;
    }
}
