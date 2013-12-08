package test;

import static org.junit.Assert.*;

import java.rmi.AccessException;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import org.junit.BeforeClass;
import org.junit.Test;

import chord.NetworkHosedException;
import chord.RMINodeClient;
import util.GenerateMultiNodeNetwork;

/**
 * Tests the storage functions of the Chord network.
 * 
 * @author Jesse Carrigan
 * 
 */
public class ChordStorageTest {

	RMINodeClient client;
	static Registry registry;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		String[] args = { "1337", "32", "4" };
		GenerateMultiNodeNetwork.main(args);
		registry = LocateRegistry.getRegistry(1337);
	}

	@Test
	public void testGetAndPut() throws AccessException, RemoteException, NotBoundException, NetworkHosedException {
		RMINodeClient client = (RMINodeClient) registry.lookup("node0");
		String key = "testKey";
		String value = "testValue";
		client.put(key, value);
		String returnValue = (String) client.get(key);
		assertEquals(value, returnValue);
	}

	@Test
	public void testGetAndPutWithDifferentClients() throws AccessException, RemoteException, NotBoundException, NetworkHosedException {
		RMINodeClient putClient = (RMINodeClient) registry.lookup("node0");
		String key = "testKey";
		String value = "testValue";
		putClient.put(key, value);
		RMINodeClient getClient = (RMINodeClient) registry.lookup("node1");
		getClient.get(key);
		String returnValue = (String) putClient.get(key);
		assertEquals(value, returnValue);
	}

	@Test
	public void testDelete() throws RemoteException, NotBoundException, NetworkHosedException {
		RMINodeClient client = (RMINodeClient) registry.lookup("node0");
		String key = "testKey";
		String value = "testValue";
		client.put(key, value);
		assertEquals(value, client.get(key));
		client.delete(key);
		String nullValue = (String) client.get(key);
		assertEquals(null, nullValue);
	}

	@Test
	public void testDeleteWithDifferentClients() throws RemoteException, NotBoundException, NetworkHosedException {
		RMINodeClient putClient = (RMINodeClient) registry.lookup("node0");
		String key = "testKey";
		String value = "testValue";
		putClient.put(key, value);
		assertEquals(value, putClient.get(key));
		RMINodeClient deleteClient = (RMINodeClient) registry.lookup("node1");
		deleteClient.delete(key);
		assertEquals(null, putClient.get(key));
	}
}
