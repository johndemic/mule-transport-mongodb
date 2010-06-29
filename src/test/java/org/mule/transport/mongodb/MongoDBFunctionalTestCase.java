package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import org.mule.api.context.notification.EndpointMessageNotificationListener;
import org.mule.api.context.notification.ServerNotification;
import org.mule.context.notification.EndpointMessageNotification;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MongoDBFunctionalTestCase extends FunctionalTestCase {

    CountDownLatch latch;

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


        muleContext.registerListener(new EndpointMessageNotificationListener() {
            public void onNotification(final ServerNotification notification) {
                final EndpointMessageNotification messageNotification = (EndpointMessageNotification) notification;
                if (messageNotification.getEndpoint().getName().equals("endpoint.mongodb.stuff")) {
                    latch.countDown();
                }
            }
        });
    }

    public void testCanInsertStringAndRequestData() throws Exception {
        latch = new CountDownLatch(1);

        MuleClient client = new MuleClient();
        String payload = "{\"name\": \"Johnny Five\"}";
        client.send("vm://input", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff", 15000).getPayload();
        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");
    }

    public void testCanInsertMapAndRequestData() throws Exception {
        latch = new CountDownLatch(1);

        MuleClient client = new MuleClient();
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "Johnny Five");
        client.send("vm://input", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
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
