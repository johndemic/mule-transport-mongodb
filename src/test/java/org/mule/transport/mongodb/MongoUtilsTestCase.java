package org.mule.transport.mongodb;

import com.mongodb.BasicDBObject;
import junit.framework.TestCase;
import org.bson.types.ObjectId;
import org.codehaus.jackson.map.ObjectMapper;

import java.util.Map;

public class MongoUtilsTestCase extends TestCase {

    public void testCanScrubQueryObjectId() throws Exception {

        ObjectMapper mapper = new ObjectMapper();

        BasicDBObject query = mapper.readValue(
                "{\"seller.$id\":{\"_id\":\"4d9351b1f4d505377f070000\"},\"deletedAt\":null, \"status\":\"active\"}",
                BasicDBObject.class);

        query = MongoUtils.scrubQueryObjectId(query);

        Map sellerId = (Map) query.get("seller.$id");

        assertTrue(sellerId.get("_id") instanceof ObjectId);
        assertNull(query.get("deletedAt"));
        assertEquals("active", query.get("status"));

    }


}
