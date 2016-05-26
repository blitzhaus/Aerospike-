/*
 * Copyright 2012-2016 Aerospike, Inc.
 *
 * Portions may be licensed to Aerospike, Inc. under one or more contributor
 * license agreements WHICH ARE COMPATIBLE WITH THE APACHE LICENSE, VERSION 2.0.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package aerospike;

import java.util.ArrayList;
import java.util.List;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;

public class OperateList extends Example {

	public OperateList(Console console) {
		super(console);
	}

	/**
	 * Perform operations on a list bin.
	 */
	@Override
	public void runExample(AerospikeClient client, Parameters params) throws Exception {
		if (! params.hasCDTList) {
			console.info("CDT list functions are not supported by the connected Aerospike server.");
			return;
		}	
		runSimpleExample(client, params);
	}

	/**
	 * Simple example of list functionality.
	 */
	public void runSimpleExample(AerospikeClient client, Parameters params) throws Exception {
		Key key = new Key(params.namespace, params.set, "listkey");
		String binName = params.getBinName("listbin");
		
		// Delete record if it already exists.
		client.delete(params.writePolicy, key);
		
		List<Value> inputList = new ArrayList<Value>();
		inputList.add(Value.get(55));
		inputList.add(Value.get(77));
		
		// Write values to empty list.
		Record record = client.operate(params.writePolicy, key, 
				ListOperation.appendItems(binName, inputList)
				);
		
		console.info("Record: " + record);			
			
		// Pop value from end of list and also return new size of list.
		record = client.operate(params.writePolicy, key, 
				ListOperation.pop(binName, -1),
				ListOperation.size(binName)
				);
		
		console.info("Record: " + record);			

		// There should be one result for each list operation on the same list bin.
		// In this case, there are two list operations (pop and size), so there 
		// should be two results.
		List<?> list = record.getList(binName);
		
		for (Object value : list) {
			console.info("Received: " + value);			
		}
	}
}