package org.mule.transport.mongodb;


import com.mongodb.DB;
import com.mongodb.Mongo;
import org.mule.api.context.notification.EndpointMessageNotificationListener;
import org.mule.api.context.notification.ServerNotification;
import org.mule.context.notification.EndpointMessageNotification;
import org.mule.module.client.MuleClient;
import org.mule.tck.FunctionalTestCase;

import java.net.URLEncoder;
import java.util.List;
import java.util.concurrent.CountDownLatch;

public class MongoDBReplicaSetFunctionalTestCase extends FunctionalTestCase {

    CountDownLatch latch;

    @Override
    protected String getConfigResources() {
        return "mongodb-replica-set-config.xml";
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

    public void testCanRequestWithQuery() throws Exception {
        MuleClient client = new MuleClient(muleContext);
        client.send("mongodb://stuff", "{\"name\": \"Johnny Five\"}", null);
        client.send("mongodb://stuff", "{\"name\": \"foo\"}", null);

        List results = (List) client.request("mongodb://stuff?query=" +
                URLEncoder.encode("{\"name\":\"foo\"}", "UTF-8"), 15000).getPayload();

        assertTrue(results.size() > 0);
    }

}
