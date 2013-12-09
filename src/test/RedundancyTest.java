package test;

import static org.junit.Assert.assertEquals;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.BeforeClass;
import org.junit.Test;

import util.GenerateMultiNodeNetwork;
import util.Log;
import chord.KeyHash;
import chord.NetworkHosedException;
import chord.RMINodeClient;
import chord.RMINodeServer;

public class RedundancyTest {

	RMINodeClient client;
	static Registry registry;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String[] args = { "1337", "16", "4" };
		GenerateMultiNodeNetwork.main(args);
		registry = LocateRegistry.getRegistry(1337);
	}

	@Test
	public void testDataRedundancy() throws AccessException, RemoteException, NotBoundException, NetworkHosedException {
		RMINodeClient putClient = (RMINodeClient) registry.lookup("node1");
		String key = "testKey";
		String value = "testValue";
		for (int i = 0; i < 10; i++) {
			String newKey = key + i;
			String newValue = value + i;
			Log.out("Add " + newKey + ", "+ newValue);
			putClient.put(newKey, newValue);
		}

		// Figure out which node we are going to forcibly remove
		KeyHash<String> hash = new KeyHash<>(key, 4);
		long nodeKey = hash.getHash();
		String nodeToRemove = "node" + nodeKey;
		Log.out("Key "+ key + " hashed to node" + nodeKey);
		
		// Drop the client without warning
		RMINodeServer deadClient = (RMINodeServer) registry.lookup(nodeToRemove);
		deadClient.leave();
		
		for (int i = 0; i < 10; i++) {
			String newKey = key + i;
			String newValue = value + i;
			String returnValue = (String) putClient.get(newKey);
			// Returns the value we added if the node redundancy is working
			Log.out("Getting " + newKey);
			assertEquals(newValue, returnValue);
		}

	}
}
