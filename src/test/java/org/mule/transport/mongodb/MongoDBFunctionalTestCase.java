package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDBFunctionalTestCase extends FunctionalTestCase {

    @Override
    protected String getConfigResources() {
        return "mongodb-functional-test-config.xml";
    }

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();

        logger.debug("Dropping collection");
        Mongo m = new Mongo("192.168.1.11", 27017);
        DB db = m.getDB("mule-mongodb");
        db.getCollection("stuff").drop();
    }

    public void testCanInsertAndRequestData() throws Exception {
        MuleClient client = new MuleClient();
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "Johnny Five");
        client.send("vm://input", payload, null);

        List results = (List) client.request("mongodb://stuff", 15000).getPayload();

        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");

    }

    public void testCanPollForData() throws Exception {
        MuleClient client = new MuleClient();

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "Johnny Five");
        client.send("vm://input", payload, null);

        assertNotNull(client.request("mongodb://stuff", 15000).getPayload());


        List results = (List) client.request("vm://output", 15000).getPayload();
        assertNotNull(results);
        assertTrue(results.size() == 1);

        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");

    }

   
}
