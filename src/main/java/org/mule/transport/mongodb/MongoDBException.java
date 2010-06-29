package org.mule.transport.mongodb;

public class MongoDBException extends RuntimeException {

    public MongoDBException() {
        super();    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MongoDBException(Throwable cause) {
        super(cause);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MongoDBException(String message) {
        super(message);    //To change body of overridden methods use File | Settings | File Templates.
    }

    public MongoDBException(String message, Throwable cause) {
        super(message, cause);    //To change body of overridden methods use File | Settings | File Templates.
    }
}
