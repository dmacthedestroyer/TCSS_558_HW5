package util;

import java.io.IOException;
import java.rmi.AlreadyBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.Random;

import chord.RMINode;
import chord.RMINodeServer;

/**
 * Script that creates a Chord network with the specified number of nodes. This
 * is good for testing how the network gets created and whether it's correct.
 * 
 * @author dmac
 * 
 */
public class MultiRandomNodeTest {
	public static void main(String[] args) throws AlreadyBoundException, IOException {
		if (args.length != 3) {
			Log.err("Usage: MultiRandomNodeTest <port> <m> <node count>");
		} else {
			int port = Integer.parseInt(args[0]);
			int m = Integer.parseInt(args[1]);
			int nodeCount = Integer.parseInt(args[2]);

			long keyspace = (long) Math.pow(2, m);
			if (nodeCount > keyspace)
				throw new IllegalArgumentException("node count (" + nodeCount + ") cannot exceed max number of nodes (" + keyspace + ")");

			Log.out(String.format("port:%s m:%s node count:%s", port, m, nodeCount));

			// we'll pull random node ids from this set
			RandomNumberSet randoms = new RandomNumberSet(keyspace);

			Registry registry = LocateRegistry.createRegistry(port);
			Log.out("Initialized registry on port " + port);

			// keep track of already created nodes so we can join nodes to any
			// arbitrary node already in the network
			ArrayList<RMINodeServer> nodes = new ArrayList<RMINodeServer>();
			Random random = new Random();

			for (int i = 0; i < nodeCount; i++) {
				RMINode nodeI = new RMINode(m, randoms.next());
				Log.out("" + nodeI.getNodeKey());
				registry.bind("" + nodeI.getNodeKey(), UnicastRemoteObject.exportObject(nodeI, 0));

				RMINodeServer fromNetwork = null;
				if (nodes.size() > 0)
					fromNetwork = nodes.get(random.nextInt(nodes.size()));

				nodeI.join(fromNetwork);
				nodes.add(nodeI);
			}

			String[] list = registry.list();
			Log.out("" + list.length);
			for (int i = 0; i < list.length; i++) {
				Log.out(String.format("%s: %s", i, list[i]));
			}
		}
	}
}
