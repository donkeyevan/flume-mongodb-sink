package com.anansi.flume.sink;

import java.net.UnknownHostException;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventHelper;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

public class MongodbSink extends AbstractSink implements Configurable{
	 private static final Logger logger = LoggerFactory.getLogger(MongodbSink.class);
	  private String serverName;
	  private int serverPort;
	  private String dbName;
	  private String collName;
	  private Mongo mongo;
	  private DB db;
	  private DBCollection collection;
	  @Override
	  public void configure(Context context) {
		Gson gson = new GsonBuilder().serializeNulls().create();
		System.out.println( gson.toJson( context.getParameters() ) );
	    serverName = context.getString("mongodb.serverName");
	    serverPort = context.getInteger( "mongodb.serverPort");
	    dbName = context.getString("mongodb.dbName" );
	    collName = context.getString( "mongodb.collName" );
	  
	  }
	  @Override
	  public void start() {
		  super.start();
		  this.initMongodb( this.serverName, this.serverPort, this.dbName, this.collName);
	  }
	  @Override
	  public void stop () {
		  super.stop();
		  mongo.close();
	  }
	  @Override
	  public Status process() throws EventDeliveryException {
		  Status result = Status.READY;
		    Channel channel = getChannel();
		    Transaction transaction = channel.getTransaction();
		    Event event = null;

		    try {
		      transaction.begin();
		      event = channel.take();
		    
		      if (event != null) {
		        if (logger.isInfoEnabled()) {
		          logger.info("Event: " + EventHelper.dumpEvent(event));
		        }
		    	    String body = new String(event.getBody());
		    	    System.out.println( "body:"+body );
		    	    DBObject entry = (DBObject) JSON.parse(body);
		    	    
		    	    WriteResult wr = collection.insert(entry);
		    	    if( wr.getLastError().containsField("code") ){	
		    			System.out.print( wr.getLastError() );
		    		}
		      } else {
		        result = Status.BACKOFF;
		      }
		      transaction.commit();
		    } catch (Exception ex) {
		      transaction.rollback();
		      throw new EventDeliveryException("Failed to log event: " + event, ex);
		    } finally {
		      transaction.close();
		    }
		    return result;
	 }
	  
	 public void initMongodb(String serverName, int serverPort, String dbName, String collName){
		  try {
			  logger.info("serverName:"+serverName+" serverPort:"+serverPort+" dbName:"+dbName+" collName:"+collName );
		      mongo = new Mongo(serverName, serverPort);
		      db = mongo.getDB(dbName);
		      collection = db.getCollection(collName);
		    } catch (UnknownHostException e) {
		      logger.error("Could not find specified server.", e);
		    } catch (MongoException e) {
		      logger.error("Error connecting to server.", e);
		    }
	 }
}

	
	

