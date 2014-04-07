package edu.sjsu.cmpe.procurement.jobs;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.yammer.dropwizard.client.JerseyClientBuilder;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;
import org.fusesource.stomp.jms.message.StompJmsMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.spinscale.dropwizard.jobs.Job;
import de.spinscale.dropwizard.jobs.annotations.Every;
import edu.sjsu.cmpe.procurement.ProcurementService;
import edu.sjsu.cmpe.procurement.domain.Book;
import edu.sjsu.cmpe.procurement.domain.BookOrders;
import edu.sjsu.cmpe.procurement.domain.ShippedBooks;


/**
 * This job will run at every 5 minutes.
 */
@Every("60s")
public class ProcurementSchedulerJob extends Job {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private int numMessages = 0;
	private List<Integer> isbns = new ArrayList<Integer>();


	public List<Integer> getIsbns() {
		return isbns;
	}


	public void addIsbn(int isbn) {
		isbns.add(isbn);
	}

	public void removeIsbn(List<Integer> isbnFromQueue){
		for (int i =0; i<isbnFromQueue.size(); i++)
			isbns.remove(i);
	}

	public void incrementNumMessages() {
		 numMessages++;
	}

	public int getNumMessages(){
		return numMessages;
	}


    @Override
    public void doJob() {
    	try {
			pullMessageFromQueue();
		} catch (JMSException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}	

		ShippedBooks shippedBooks = getDataFromPublisher();

		for (int i = 0; i<shippedBooks.getNumBooks();i++){
			String category = shippedBooks.getShipped_books().get(i).getCategory();
			try {
				publishBooks(shippedBooks.getShipped_books().get(i),category);
			} catch (JMSException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
    	
	String strResponse = ProcurementService.jerseyClient.resource(
		"http://ip.jsontest.com/").get(String.class);
	log.debug("Response from jsontest.com: {}", strResponse);


    }


	private void pullMessageFromQueue() throws JMSException {
		// TODO Auto-generated method stub


		String user = env("APOLLO_USER", "admin");
		String password = env("APOLLO_PASSWORD", "password");
		String host = env("APOLLO_HOST", "54.215.133.131");
		int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
		String queue = "/queue/67921.book.orders";
		String destination = arg(0, queue);

		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);

		Connection connection = factory.createConnection(user, password);
		connection.start();

		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		Destination dest = new StompJmsDestination(destination);
		MessageConsumer consumer = session.createConsumer(dest);

		System.out.println("Waiting for messages from " + queue + "...");
		long waitUntil = 5000; // wait for 5 sec
		Message msg = null;
		String body = null;
		while(true) {
		    //Message msg;
			try {
				msg = consumer.receive(waitUntil);
			} catch (JMSException e) {
				e.printStackTrace();
			}
		    if( msg instanceof  TextMessage ) {
		           //String body;
				try {
					body = ((TextMessage) msg).getText();
				} catch (JMSException e) {
					e.printStackTrace();
				}
		           System.out.println("Received message = " + body);	       

		           //get ISBN from message and add it to list of ISBNs
		           addIsbn(Integer.parseInt(body.split(":")[1]));
		           incrementNumMessages();

		    } 
		   		    else if (msg == null) {
		          System.out.println("No new messages. Exiting due to timeout - " + waitUntil / 1000 + " sec");
		          if (getNumMessages()>0){
		        	  sendOrder(isbns);

		          }

		          break;
		    } 
		    else {
		         System.out.println("Unexpected message type: " + msg.getClass());
		    }
		} // end while loop
		try {
			connection.close();
		} catch (JMSException e) {
			e.printStackTrace();
		}
		System.out.println("Done");


	}
public void sendOrder(List<Integer> isbnFromQueue){

		BookOrders bookOrder = new BookOrders();
		bookOrder.setId("67921");
		bookOrder.setOrderBookIsbns(isbnFromQueue);

		Client client = Client.create();
		WebResource webResource = client.resource("http://54.215.133.131:9000/orders");
		ClientResponse response = webResource.type("application/json").post(ClientResponse.class,bookOrder);

		if(response.getStatus() == 200){
			numMessages = 0;
			removeIsbn(isbns);
			String outputResponse = response.getEntity(String.class);
			System.out.println(outputResponse);
			System.out.println("Status returned on POST: " + response.getStatus());	
			
		}
		else{
			System.out.println("Post Uncuccessful ");
		}

	}
public void publishBooks(Book book, String category) throws JMSException{

	String user = env("APOLLO_USER", "admin");
	String password = env("APOLLO_PASSWORD", "password");
	String host = env("APOLLO_HOST", "54.215.133.131");
	int port = Integer.parseInt(env("APOLLO_PORT", "61613"));
	String destination = arg(0, "/topic/67921.book."+ category);

	StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
	factory.setBrokerURI("tcp://" + host + ":" + port);

	Connection connection = factory.createConnection(user, password);
	connection.start();
	Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
	Destination dest = new StompJmsDestination(destination);
	MessageProducer producer = session.createProducer(dest);
	producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

	TextMessage msg = session.createTextMessage(createMessage(book));
	msg.setLongProperty("id", System.currentTimeMillis());
	producer.send(msg);

	System.out.println(msg.toString());
	connection.close();
}

public ShippedBooks getDataFromPublisher(){

	Client client =  Client.create();
	WebResource webResource = client.resource("http://54.215.133.131:9000/orders/67921");		
	ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
	ShippedBooks shippedBooks = response.getEntity(ShippedBooks.class);
	System.out.println("Status returned on GET: " + response.getStatus());
	return shippedBooks;
}

public String createMessage (Book shippedBook){

	String message = shippedBook.getIsbn()+":"+shippedBook.getTitle()+":"+shippedBook.getCategory()+":"+shippedBook.getCoverimage();	
	return message;
}


private static String env(String key, String defaultValue) {
String rc = System.getenv(key);
if( rc== null ) {
    return defaultValue;
}
return rc;
}

private static String arg(int index, String defaultValue) {
    return defaultValue;	
}
}
