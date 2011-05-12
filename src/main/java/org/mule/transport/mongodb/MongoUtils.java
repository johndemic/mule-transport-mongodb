package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import org.bson.types.ObjectId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MongoUtils {

    /**
     * Replaces String instances of _id in a query with an instance of <code>ObjectId</code>.
     *
     * @param query the query to scrub
     * @return the scrubbed query.
     */
    public static BasicDBObject scrubQueryObjectId(BasicDBObject query) {

        if (query.containsField("_id")) {
            ObjectId id = new ObjectId(query.get("_id").toString());
            query.removeField("_id");
            query.put("_id", id);
        }

        List<String> mapFields = new ArrayList<String>();

        for (String field : query.keySet()) {
            if (query.get(field) instanceof Map)
                mapFields.add(field);
        }

        for (String field : mapFields) {
            BasicDBObject scrubbedObject = scrubQueryObjectId(new BasicDBObject( (Map) query.get(field)));
            query.remove(field);
            query.put(field, scrubbedObject);
        }

        return query;
    }
}
