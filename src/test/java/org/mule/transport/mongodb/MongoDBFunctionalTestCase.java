package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFSFile;
import org.bson.types.ObjectId;
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
import java.math.BigInteger;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.util.ArrayList;
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
    protected void suitePreSetUp() throws Exception {
        super.suitePreSetUp();
    }

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();

        latch = new CountDownLatch(1);

        logger.debug("Dropping database");
        Mongo m = new Mongo("localhost", 27017);
        DB db = m.getDB("mule-mongodb");
        db.dropDatabase();
        db.requestDone();

        muleContext.registerListener(new EndpointMessageNotificationListener() {
            public void onNotification(final ServerNotification notification) {
                final EndpointMessageNotification messageNotification = (EndpointMessageNotification) notification;

                if (messageNotification.getEndpoint().equals("mongodb://stuff")) {
                    latch.countDown();
                }
            }
        });

    }

    public void testCanInsertStringAndRequestResults() throws Exception {

        MuleClient client = new MuleClient(muleContext);
        String payload = "{\"name\": \"Johnny Five\"}";
        MuleMessage result = client.send("vm://input", payload, null);

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff", 15000).getPayload();
        assertNotNull(results);
        Map resultMap = (Map) results.get(0);
        assertEquals(resultMap.get("name"), "Johnny Five");
    }

    public void testCanInsertStringIntoSubCollectionAndRequestResults() throws Exception {

        MuleClient client = new MuleClient(muleContext);
        String payload = "{\"name\": \"Johnny Five Sub\"}";
        MuleMessage response = client.send("mongodb://stuff.sub", payload, null);

        assertNotNull(response.<Object>getOutboundProperty("objectId"));

        assertNotNull(response);
        Map responseMap = (Map) response.getPayload();
        assertNotNull(responseMap.get("_id"));
//        assertNotNull(responseMap.get("_ns"));
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        List results = (List) client.request("mongodb://stuff.sub", 15000).getPayload();
        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five Sub");
    }


    public void testCanUpdateStringWhenObjectIdDoesntExist() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        String payload =
                "{\"_id\": " + '"' + new ObjectId().toStringMongod() + '"' + ",\"age\": \"1\",\"name\": \"John the Fifth\"}";


        client.send("vm://input", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff", 15000).getPayload();
        assertEquals(1, results.size());
    }

    public void testCanUpdateStringAndRequestResults() throws Exception {

        MuleClient client = new MuleClient(muleContext);
        String payload = "{\"name\": \"Johnny Five\"}";
        client.send("vm://input", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff", 15000).getPayload();
        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");

        String id = result.get("_id").toString();
        assertNotNull(id);

        String newPayload = "{\"_id\": " + '"' + id + '"' + ",\"age\": \"1\",\"name\": \"John the Fifth\"}";
        latch = new CountDownLatch(2);
        client.send("vm://input", newPayload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        results = (List) client.request("mongodb://stuff", 15000).getPayload();
        assertNotNull(results);
        result = (Map) results.get(0);

        assertEquals(1, results.size());
        assertEquals(id, result.get("_id").toString());
        assertEquals("1", result.get("age"));
        assertEquals("John the Fifth", result.get("name"));

    }


    public void testCanRequestWithQuery() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        client.send("mongodb://stuff", "{\"name\": \"Johnny Five\"}", null);
        client.send("mongodb://stuff", "{\"name\": \"foo\"}", null);

        List results = (List) client.request("mongodb://stuff?query=" +
                URLEncoder.encode("{\"name\":\"foo\"}", "UTF-8"), 15000).getPayload();

        assertEquals(1, results.size());
    }

    public void testCanInsertFileIntoGridFs() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        MuleMessage result = client.send("mongodb://bucket:somefiles", new File("src/test/resources/test.dat"), null);
        assertNotNull(result);
        assertNotNull(result.<Object>getOutboundProperty("objectId"));
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }


    public void testCanRequestFilesWithQuery() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        File sourceFile = new File("src/test/resources/test.dat");

        MuleMessage result = client.send("mongodb://bucket:somefiles", sourceFile, null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());

        assertEquals(getMD5(FileUtils.readFileToByteArray(sourceFile)), file.getMD5());


        List results = (List) client.request("mongodb://bucket:somefiles?query=" +
                URLEncoder.encode("{\"filename\":\"foo\"}", "UTF-8"), 15000).getPayload();

        assertEquals(0, results.size());
    }


    public void testCanInsertListOfMapsAndRequestData() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        List<Map<String, String>> maps = new ArrayList<Map<String, String>>();

        for (int i = 0; i < 5; i++) {
            Map<String, String> payload = new HashMap<String, String>();
            payload.put("name", "Johnny " + i);
            maps.add(payload);
        }

        client.send("mongodb://stuff.map", maps, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff.map", 15000).getPayload();

        assertNotNull(results);
        assertEquals(5, results.size());

        for (int i = 0; i < 5; i++) {
            Map result = (Map) results.get(i);
            assertEquals("Johnny " + i, result.get("name"));
        }
    }


    String getMD5(byte[] bytes) throws Exception {
        MessageDigest m = MessageDigest.getInstance("MD5");
        m.update(bytes, 0, bytes.length);
        return new BigInteger(1, m.digest()).toString(16);
    }


    public void testCanPollForFiles() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        MuleMessage result = client.send("mongodb://bucket:somefiles", new File("src/test/resources/test.dat"), null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());

        result = client.send("mongodb://bucket:somefiles", new File("src/test/resources/test.dat"), null);
        assertNotNull(result);
        file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());

        List results = null;
        int count = 0;

        while (results == null || (results.size() == 0 && count < 5)) {
            results = (List) client.request("vm://output.somefiles", 15000).getPayload();
            Thread.sleep(5000);
            count++;
        }
        assertNotNull(results);
        assertTrue(results.size() > 0);
    }


    /*
   public void testCanInsertStringIntoSubCollectionWithExpressionAndRequestResults() throws Exception {

       MuleClient client = new MuleClient(muleContext);
       String payload = "{\"name\": \"Johnny Five Foo\"}";
       Map properties = new HashMap();
       properties.put("collectionHeader", "foo");
       client.send("vm://input.sub.expr", payload, properties);
       assertTrue(latch.await(5, TimeUnit.SECONDS));
       List results = (List) client.request("mongodb://stuff.sub.foo", 15000).getPayload();
       assertNotNull(results);
       Map result = (Map) results.get(0);
       assertEquals(result.get("name"), "Johnny Five Foo");
   } */


    public void testCanRequestFiles() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        MuleMessage result = client.send("mongodb://bucket:somefiles", new File("src/test/resources/test.dat"), null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertEquals("test.dat", file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());

        List results = (List) client.request("mongodb://bucket:somefiles", 15000).getPayload();
        assertNotNull(results);
    }


    public void testCanPollForData() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "Johnny Five");
        client.send("vm://input", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));

        List results = null;
        int count = 0;

        while (results == null || (results.size() == 0 && count < 5)) {
            results = (List) client.request("vm://output", 15000).getPayload();
            Thread.sleep(5000);
            count++;
        }

        assertNotNull(results);
        assertEquals(1, results.size());

        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");
    }


    public void testCanInsertInputStreamWithOutFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        InputStream stream = new FileInputStream(new File("src/test/resources/test.dat"));
        MuleMessage result = client.send("mongodb://bucket:somefiles", stream, null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertNull(file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertInputStreamWithFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient(muleContext);

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
        MuleClient client = new MuleClient(muleContext);

        byte[] input = FileUtils.readFileToByteArray(new File("src/test/resources/test.dat"));


        MuleMessage result = client.send("mongodb://bucket:somefiles", input, null);
        assertNotNull(result);
        GridFSFile file = (GridFSFile) result.getPayload();
        assertNull(file.getFilename());
        assertEquals("b6d81b360a5672d80c27430f39153e2c", file.getMD5());
    }

    public void testCanInsertByteArrayWithFileNameIntoGridFs() throws Exception {
        MuleClient client = new MuleClient(muleContext);

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
        MuleClient client = new MuleClient(muleContext);

        boolean exceptionThrown = false;

        try {
            client.send("mongodb://bucket:somefiles", 1234.56, null);
        } catch (DispatchException e) {
            exceptionThrown = true;
        }
        assertTrue(exceptionThrown);
    }


    public void testCanDispatch() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        String payload = "{\"name\": \"Johnny Fivethousand\"}";
        client.dispatch("mongodb://stuff", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
    }


    public void testCanInsertMapAndRequestData() throws Exception {

        MuleClient client = new MuleClient(muleContext);
        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "Johnny Five");
        client.send("mongodb://stuff.map", payload, null);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        List results = (List) client.request("mongodb://stuff.map", 15000).getPayload();

        assertNotNull(results);
        Map result = (Map) results.get(0);
        assertEquals(result.get("name"), "Johnny Five");
    }


}
