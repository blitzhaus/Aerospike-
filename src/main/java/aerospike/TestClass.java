package aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Host;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.ClientPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;

public class TestClass {
	
	Host[] hosts = new Host[]{
			new Host("127.0.0.1", 3000),
			
		};
	AerospikeClient client = new AerospikeClient(new ClientPolicy(), hosts);

	
	public TestClass(){
		for(int i=0; i < 50000000; i++){
			writeTo();
			readFrom();
		}
		
		client.close();
	}
	private void readFrom() {
		// TODO Auto-generated method stub
		
		Record rec = client.get(null, key, "username", "password", "tweetCount");
		System.out.println(rec.toString());
		rec = client.get(null, key1);
		System.out.println(rec.toString());
	}
	
	WritePolicy wPolicy =  new WritePolicy();
	Key key1;
	Key key;
	Bin b1;
	Bin b2;
	private void writeTo() {
		// TODO Auto-generated method stub
	
		key = new Key("test", "users", 1);
		
		
		wPolicy.recordExistsAction = RecordExistsAction.UPDATE;
		wPolicy.maxRetries = 50;
		
		
		b1 = new Bin("username", 1);
		b2 = new Bin("password", "ajith");
		
		client.put(wPolicy, key, b1, b2);
		
		Bin counter = new Bin("tweetCount", 1);
		client.add(wPolicy, key, counter);
		
		
		key1 = new Key("test", "users", 2);
		Bin b3 = new Bin("greet", "Hello world");
		client.append(wPolicy, key1, b3);
		/*client.touch(wPolicy, key);
		client.touch(wPolicy, key1);*/
		
		//client.delete(wPolicy, key);
		
		
		
	}
	public static void main(String[] args){
	
			new TestClass();
			
	}
	
	
	
}
