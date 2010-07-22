package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFSFile;
import org.mule.api.MuleMessage;
import org.mule.api.context.notification.EndpointMessageNotificationListener;
import org.mule.api.context.notification.ServerNotification;
import org.mule.api.transport.DispatchException;
import org.mule.context.notification.EndpointMessageNotification;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;
import org.mule.util.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SuppressWarnings({"unchecked"})
public class MongoDBFunctionalTestCase extends FunctionalTestCase {

    CountDownLatch latch;

    @Override
    protected String getConfigResources() {
        return "mongodb-functional-test-config.xml";
    }

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();

        latch = new CountDownLatch(1);

        logger.debug("Dropping collection");
        Mongo m = new Mongo("192.168.1.11", 27017);
        DB db = m.getDB("mule-mongodb");
        db.getCollection("stuff").drop();
        db.getCollection("somefiles").drop();

        muleContext.registerListener(new EndpointMessageNotificationListener() {
            public void onNotification(final ServerNotification notification) {
                final EndpointMessageNotification messageNotification = (EndpointMessageNotification) notification;
                if (messageNotification.getEndpoint().getName().equals("endpoint.mongodb.stuff")) {
                    latch.countDown();
                }
            }
        });
    }

    public void testCanInsertFileIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();
        MuleMessage result = client.send("mongodb://bucket:somefiles", new File("src/test/resources/test.dat"), null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertInputStreamWithOutFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();

        InputStream stream = new FileInputStream(new File("src/test/resources/test.dat"));
        MuleMessage result = client.send("mongodb://bucket:somefiles", stream, null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertNull(file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertInputStreamWithFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();

        Map properties = new HashMap();
        properties.put(MongoDBConnector.PROPERTY_FILENAME, "foo.dat");
        InputStream stream = new FileInputStream(new File("src/test/resources/test.dat"));
        MuleMessage result = client.send("mongodb://bucket:somefiles", stream, properties);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("foo.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertByteArrayWithOutFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();

        byte[] input = FileUtils.readFileToByteArray(new File("src/test/resources/test.dat"));


        MuleMessage result = client.send("mongodb://bucket:somefiles", input, null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertNull(file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertByteArrayWithFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();

        byte[] input = FileUtils.readFileToByteArray(new File("src/test/resources/test.dat"));
        Map properties = new HashMap();
        properties.put(MongoDBConnector.PROPERTY_FILENAME, "foo.dat");

        MuleMessage result = client.send("mongodb://bucket:somefiles", input, properties);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("foo.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCannotInsertUnsupportedPayloadIntoGridFs() throws Exception {
        MuleClient client = new MuleClient();

        boolean exceptionThrown = false;

        try {
            client.send("mongodb://bucket:somefiles", 1234.56, null);
        } catch (DispatchException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }


    public void testCanInsertStringIntoSubCollectionWithExpressionAndRequestResults() throws Exception {

        MuleClient client = new MuleClient();
        String payload = "{\"name\": \"Johnny Five Foo\"}";
        Map properties = new HashMap();
        properties.put("collectionHeader", "foo");
        client.send("vm://input.sub.expr", payload, properties);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff.sub.foo", 15000).getPayload();
        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five Foo");
    }


    public void testCanInsertStringIntoSubCollectionAndRequestResults() throws Exception {

        MuleClient client = new MuleClient();
        String payload = "{\"name\": \"Johnny Five Sub\"}";
        client.send("vm://input.sub", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff.sub", 15000).getPayload();
        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five Sub");
    }

    public void testCanDispatch() throws Exception {
        MuleClient client = new MuleClient();
        String payload = "{\"name\": \"Johnny Fivethousand\"}";
        client.dispatch("mongodb://stuff", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }

    public void testCanInsertStringAndRequestResults() throws Exception {

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
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        assertNotNull(client.request("mongodb://stuff", 15000).getPayload());


        List results = (List) client.request("vm://output", 15000).getPayload();
        assertNotNull(results);
        assertTrue(results.size() == 1);

        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");

    }


}
