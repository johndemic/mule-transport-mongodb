package org.mule.transport.mongodb;

import com.mongodb.gridfs.GridFSFile;
import org.mule.api.MuleContext;
import org.mule.transport.AbstractMuleMessageFactory;

import java.util.List;
import java.util.Map;

public class MongoDBMessageFactory extends AbstractMuleMessageFactory {

    public MongoDBMessageFactory(MuleContext context) {
        super(context);
    }

    @Override
    protected Class<?>[] getSupportedTransportMessageTypes() {
        return new Class[]{ Map.class, List.class, GridFSFile.class  };
    }

    @Override
    protected Object extractPayload(Object o, String s) throws Exception {
        return o;
    }
}
