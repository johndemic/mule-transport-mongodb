Mule MongoDB Transport
======================

Overview
--------
The MongoDB transport allows you to insert and retrieve data from MongoDB
collections and buckets.

A MongoDB inbound-endpoint accepts a JSON query that is used to poll a MongoDB
collection or bucket at the specified interval for results. The resultant
message can then be processed by a component or routed to an outbound-endpoint.

The MongoDB outbound-endpoint inserts either Map or JSON data into the specified
collection. Files, streams and byte arrays can be persisted as BSON data using
the inbound-endpoint with a bucket. See the documentation on GridFS below for
details.

Please open a JIRA if you experience any issues. For general comments,
suggestions, etc I can be reached at john.demic@gmail.com.

Installation
------------

Add the following to your project's pom.xml:

```xml
<dependency>
    <groupId>org.mule.transports</groupId>
    <artifactId>mule-transport-mongodb</artifactId>
    <version>3.1.2.0</version>
</dependency>
```

Examples
--------

The following examples will demonstrate using MongoDB inbound and outbound
endpoints in conjunction with the STDIO transport. Note that the configs omit
the namespace declarations for clarity.

```xml
<mule>

    <mongodb:connector name="mongodbConnector"
                       database="mule-mongodb"
                       username="user"
                       password="pass"
                       pollingFrequency="5000"
                       uri="mongodb://localhost"
            >
    </mongodb:connector>

   <stdio:connector name="stdioConnector" 
             promptMessage="Enter some JSON"/>

    <model>
        <service name="MongoDB Test Input Service">
            <inbound>
                <stdio:inbound-endpoint system="IN"/>
            </inbound>
            <outbound>
                <pass-through-router>
                    <mongodb:outbound-endpoint collection="stuff"/>
                </pass-through-router>
            </outbound>
        </service>
    </model>
</mule>
```xml

Starting Mule will produce a prompt to enter some JSON which will be inserted
into the "stuff" collection belonging to the "mule-mongodb" database:

```
Enter some JSON
{"hello": "world"}
INFO  2010-06-28 22:24:03,339 
[mongodbConnector.dispatcher.1] 
org.mule.transport.mongodb.MongoDBMessageDispatcher: Connected: 
endpoint.outbound.mongodb://stuff
```

You can also dispatch Map payloads to insert into MongoDB. The following
illustrates this using a nested Map with MuleClient in a Groovy script:

```groovy
def client = new MuleClient();
def person = [name: "Johnny Five", address: [city: "Brooklyn", state: "NY"]]
client.dispatch("mongodb://stuff", person, null);
```

Updating Documents
------------------

Documents can also be updated with an outbound-endpoint. In order to update data
you need to set the "dispatch_mode" property on the outbound-message to
"update". If the payload of the message contains an "_id" key then the transport
will attempt to update a document with that ID. If the document doesn't exist
then it will be inserted using the ID. To specify the object to update without
supplying an ID you can set the "update_query" with a JSON query to identify the
objects you wish to update. Here's an example:

```groovy
def client = new MuleClient();
def person = [address: [city: "Queens", state: "NY"]]
client.dispatch("mongodb://stuff", person, [dispatch_mode: "update",
update_query:"\{"name": "Johnny Five"\}"]);
```

The update will fail if the query doesn't return any documents. To instead have
the dispatcher create documents when they don't exist you can set the
"update_upsert" property on the outbound message:

```groovy
client.dispatch("mongodb://stuff", person, [dispatch_mode: "update",
update_query:"\{"name": "Johnny Five"\}"], update_upsert:"true");
```

By default the dispatcher will only update the first document returned by the
query. To have it update all the documents you can set the "update_multi"
property on the outbound message:

```groovy
client.dispatch("mongodb://stuff", person, [dispatch_mode: "update",
update_query:"\{"name": "Johnny Five"\}"], update_multi:"true");
```

Deleting Documents
------------------

Documents can be deleted by setting the "dispatch_mode" property of the
outbound-message to "delete". The payload of the message will be used to
construct the deletion query. For example:

```groovy
client.send("mongodb://stuff", "{\"name\": \"Foo\"}", [dispatch_mode: "delete"];
```

Write Concerns
--------------

Write concerns are now supported by this transport. The desired WriteConcern can
be set on the "write_concern" property on a message. Allowed values are
NONE,NORMAL,SAFE,FSYNC_SAFE, and REPLICAS_SAFE. The default is NONE.

### Retrieving Data

The MongoDB transport allows you to poll a collection using a query at a
specified interval and send the results through to a component or outbound
endpoint. The following demonstrates a query that returns all objects in a
collection with a timestamp greater then January 1, 2010:


```xml          
    <model>
        <service name="MongoDB Polling Service">
            <inbound>
                <mongodb:inbound-endpoint collection="stuff" 
                              query='{ "ts": { "$gt": 1262304000000}  }'/>
            </inbound>
            <outbound>
                <pass-through-router>
                    <stdio:outbound-endpoint system="OUT"/>
                </pass-through-router>
            </outbound>
        </service>
    </model>
```

Documents matching the query will be sent to the outbound endpoint. For
instance:

```
INFO  2010-06-29 16:07:22,738 [connector.stdio.0.dispatcher.1] 
org.mule.transport.stdio.StdioMessageDispatcher: Connected: 
endpoint.outbound.stdio://system.out
[{ "_id" : { "$oid" : "4c2a513d42157f56a3d82a9c"} , "x" : 203 , "y" : 102 , "ts"
:
1277841725235}]
```

Note that the payload of these messages are instances of BasicDBObject from the
MongoDB driver. These seem to serialize to JSON when toString() is invoked,
hence the above output. This also means you can manipulate the output however
you see fit, either by interacting with BasicDBObject or serializing to JSON via
toString().

Retrieving Data with an outbound-endpoint
-----------------------------------------

Queries are also supported on outbound-endpoints. This is useful in flows,
here's an example:

```xml
    <flow name="Query Test With Expression">
        <vm:inbound-endpoint path="query.expr.in">
            <message-properties-transformer>
                <add-message-property key="FOO" value="#[header:INBOUND:FOO]"/>
            </message-properties-transformer>
        </vm:inbound-endpoint>
        <mongodb:outbound-endpoint collection="query" query='{ "name":
"#[header:FOO]" }'
                                   exchange-pattern="request-response"/>
        <vm:outbound-endpoint path="query.expr.out"/>
    </flow>
```

This demonstrates a flow that performs a query against a collection using a
expression to enrich the query.

Using GridFS
============

As of version 2.2.1.4, the MongoDB transport supports GridFS.

### Inserting into a Bucket

The following demonstrates how to use the file transport to insert files from a
directory into a GridFS bucket.

```xml
        <service name="MongoDB Test Input Service">
            <inbound>
                <file:inbound-endpoint 
                    path="/tmp/mongo"/>
            </inbound>
            <outbound>
                <pass-through-router>
                    <mongodb:outbound-endpoint 
                    bucket="stuff"/>
                </pass-through-router>
            </outbound> 
        </service>
```

You set the "filename" header of the message to set the filename of the object
in MongoDB. This is probably a good thing to set when inserting streamed data or
byte arrays.

### Polling a Bucket

The following demonstrates how to poll a GridFS bucket for files and, using the
file transport, output them to a directory.

```xml
        <service name="MongoDB Polling Service">
            <inbound>
                <mongodb:inbound-endpoint 
                    query='{ "filename": "*.dat" }' 
                    bucket="stuff"/>
            </inbound>
            <outbound>
               <list-message-splitter-router>
                    <file:outbound-endpoint path="/tmp/foo">
                        <transformers>
                            <mongodb:db-file-to-byte-array/>
                        </transformers>
                    </file:outbound-endpoint>
                </list-message-splitter-router>
            </outbound>
        </service>
```

Note that the payloads of objects coming back from GridFS will be a collection
of instances of GridFSDBFile. You can use the provided
mongodb:db-file-to-byte-array or mongodb:db-file-to-input-stream to
automatically extract either the byte array or stream from the GridFSDBFile.
These transformers will additionally propagate the "filename" property of the
GridFSDBFile to the returned message, allowing the files to be named predictably
without any additional effort.


### Getting a Single File

You can use the gridfs-file message processor to retrieve a single file from
GridFS. This is typically useful in a flow, for example:

```xml
<flow name="GridFS Load File">
    <vm:inbound-endpoint path="gridfs.load.in"/>
    <mongodb:gridfs-file bucket="foo"/>
    <vm:outbound-endpoint path="gridfs.load.out"/>
</flow>
```

By default gridfs-file will use the payload of the message as the filename to
load from GridFS. An expression can be used to control how the payload is
evaluated by setting the "fileExpression" property on the processor. You can
also avoid using the message payload as the filename altogether by setting the
"query" property on the processor, for instance:

```xml
<flow name="GridFS Query For File By ID">
    <vm:inbound-endpoint path="gridfs.query.in">
        <message-properties-transformer>
            <add-message-property key="MONGO_ID"
value="#[header:INBOUND:MONGO_ID]"/>
        </message-properties-transformer>
    </vm:inbound-endpoint>
    <mongodb:gridfs-file bucket="foo" query='{ "_id": "#[header:MONGO_ID]" }' />
    <vm:outbound-endpoint path="gridfs.query.out"/>
</flow>
```

=== Using a Requestor

You can use MuleClient to fetch GridFSDBFile from MongoDB:

```java
MuleClient client = new MuleClient();
List results = (List) client.request("mongodb://bucket:files?query=" +
URLEncoder.encode("{\"filename\":\"foo\"}","UTF-8"), 15000).getPayload();
```

Note that the JSON query needs to encoded. The results, as with the
inbound-endpoint, will be instances of GridFSDBFile.

The objectId Property
=====================

Messages sent to a collection or bucket will get an outbound property called
"objectId" set corresponding to the "_id" field of the document or file. You can
reference this property when chaining mongodb endpoints in a chaining-router or
flow. This is primarily useful in creating subsequent DBRef's in the processing
chain.
