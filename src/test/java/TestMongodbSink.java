
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.SimpleEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.anansi.flume.sink.MongodbSink;


public class TestMongodbSink {
	  private static final Logger looger = LoggerFactory
      .getLogger(TestMongodbSink.class);
	  private MongodbSink sink;
	 @Before
	  public void setUp() {
		 looger.debug("Starting...");
		 sink = new MongodbSink();
	 }
	 
	 @After
	 public void tearDown(){
		 sink.stop();
	 }
	 @Test
	 public void process() throws EventDeliveryException{

		    int totalEvents = 0;

		     Context context = new Context();

		    // context.put("hdfs.path", testPath + "/%Y-%m-%d/%H");
		    context.put("mongodb.serverName", "192.168.1.101");
		    context.put("mongodb.serverPort", "27017");
		    context.put("mongodb.dbName", "flume_city");
		    context.put("mongodb.collName", "city");
		  

		    Configurables.configure(sink, context);

		    Channel channel = new MemoryChannel();
		    Configurables.configure(channel, context);

		    sink.setChannel(channel);
		    sink.start();

		    Calendar eventDate = Calendar.getInstance();
		    List<String> bodies = new ArrayList<String>();

		    // push the event batches into channel 
		    for (int i = 1; i <= 5; i++) {
		      Transaction txn = channel.getTransaction();
		      txn.begin();
		    
		        Event event = new SimpleEvent();
		        eventDate.clear();
		        eventDate.set(2011, i, i, i, 0); // yy mm dd
		        String body = "{\"stamp\":\"2012-08-17 16:57:56 202\",\"playerId\":2367,\"playerName\":\"XXX\",\"actionName\":\"XXX\",\"actionId\":4,\"deviceId\":\"89cc297b9232c0994872da49b173cebe31446544\",\"logType\":2162816,\"properties\":{\"awardGoodsType\":753,\"result\":0,\"awardMoney\":0,\"costBrave\":1,\"awardGoodsCategory\":8,\"awardGoodsAmount\":1,\"awardExp\":6,\"isLostToolFlag\":false,\"crimeTypeId\":102}}";
		        event.setBody(body.getBytes());
		        bodies.add(body);
		        channel.put(event);
		        totalEvents++;
		      
		      txn.commit();
		      txn.close();
		      System.out.println( "total event num:"+totalEvents);
		      // execute sink to process the events
		      sink.process();
		    }

		    sink.stop();
	 }
}
