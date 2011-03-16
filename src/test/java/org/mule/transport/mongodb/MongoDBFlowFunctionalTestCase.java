package org.mule.transport.mongodb;

import com.mongodb.DB;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.gridfs.GridFS;
import com.mongodb.gridfs.GridFSDBFile;
import com.mongodb.gridfs.GridFSInputFile;
import org.mule.api.MuleMessage;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;
import org.mule.util.IOUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoDBFlowFunctionalTestCase extends FunctionalTestCase {

    DB db;

    @Override
    protected void doSetUp() throws Exception {
        super.doSetUp();
        logger.debug("Dropping database");
        Mongo m = new Mongo("localhost", 27017);
        db = m.getDB("mule-mongodb");
        db.dropDatabase();
        db.requestDone();
    }


    @Override
    protected String getConfigResources() {
        return "mongodb-global-endpoint-test-config.xml";
    }

    public void testQuery() throws Exception {


        MuleClient client = new MuleClient(muleContext);

        String payload = "{\"name\": \"John\"}";

        client.send("mongodb://query", payload, null);

        Map<String, String> properties = new HashMap<String, String>();
        properties.put("FOO", "John");
        properties.put(MongoDBConnector.MULE_MONGO_WRITE_CONCERN, MongoDBWriteConcern.SAFE.toString());

        client.dispatch("vm://query.expr.in", "", properties);

        MuleMessage result = client.request("vm://query.expr.out", 15000);

        assertNotNull(result);

        List<DBObject> results = (List<DBObject>) result.getPayload();
        assertEquals(1, results.size());

    }

    public void testUpsert() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "foo");

        client.send("vm://upsert.true", payload, null);
        client.request("vm://upsert.out.true", 10000);

        MuleMessage result = client.request("mongodb://upserts", 15000);
        assertNotNull(result);

        List resultPayload = (List) result.getPayload();
        assertNotNull(resultPayload);
        assertEquals(1, resultPayload.size());
        Map upsertedObject = (Map) resultPayload.get(0);
        assertEquals("foo", upsertedObject.get("name"));

    }

    public void testNotUpsert() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "foo");

        client.send("vm://upsert.false", payload, null);
        client.request("vm://upsert.out.false", 10000);
        MuleMessage result = client.request("mongodb://upserts", 15000);
        assertNotNull(result);

        List resultPayload = (List) result.getPayload();
        assertNotNull(resultPayload);
        assertEquals(0, resultPayload.size());

    }

    public void testGlobalEndpointOutbound() throws Exception {
        MuleClient client = new MuleClient(muleContext);

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "foo2");

        Map<String, String> properties = new HashMap<String, String>();
        properties.put(MongoDBConnector.MULE_MONGO_WRITE_CONCERN, MongoDBWriteConcern.NORMAL.toString());

        client.dispatch("mongodb://foo", payload, properties);

        MuleMessage result = client.request("vm://fooOutput", 15000);
        assertNotNull(result);

        List listResult = (List) result.getPayload();
        assertEquals(1, listResult.size());

        Map mapResult = (Map) listResult.get(0);
        assertEquals("foo2", mapResult.get("name"));
    }


    public void testGlobalEndpointInbound() throws Exception {

        MuleClient client = new MuleClient(muleContext);

        Map<String, String> payload = new HashMap<String, String>();
        payload.put("name", "foo");

        client.dispatch("vm://input", payload, null);

        MuleMessage result = client.request("vm://output", 15000);

        assertNotNull(result);

        Map mapResult = (Map) result.getPayload();
        assertEquals("foo", mapResult.get("name"));
    }

    public void testCanLoadFileFromGridFS() throws Exception {

        GridFS gridFS = new GridFS(db, "foo");

        GridFSInputFile file = gridFS.createFile(IOUtils.toInputStream("Hello, world."), "foo.txt");
        file.save();

        MuleClient client = new MuleClient(muleContext);

        client.dispatch("vm://gridfs.load.in", "foo.txt", null);
        MuleMessage result = client.request("vm://gridfs.load.out", 15000);

        GridFSDBFile outputFile = (GridFSDBFile) result.getPayload();

        assertNotNull(result);
        assertEquals("foo.txt", outputFile.getFilename());
        assertEquals("Hello, world.", IOUtils.toString(outputFile.getInputStream()));


    }


}
