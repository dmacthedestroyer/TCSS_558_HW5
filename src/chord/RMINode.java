package chord;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import util.Log;

/**
 * Implements a node in the Chord network.
 * 
 * @author Daniel McDonald
 */
public class RMINode implements RMINodeServer, RMINodeState {

	private static final int FIX_FINGER_INTERVAL = 1000;
	private final int networkRetries;
	private final int hashLength;
	private final long keySpace;
	private final long nodeKey;
	private final NodeFileLogger logger;
	private FingerTable fingerTable;
	private final Map<Long, Serializable> nodeStorage = new ConcurrentHashMap<>();
	private RMINodeServer predecessor;
	private boolean hasNodeLeft;
	private final RingRange ringRange = new RingRange();
	private final Thread backgroundThread = new Thread() {
		{
			setDaemon(true);
		}

		public void run() {
			while (!isInterrupted()) {
				try {
					synchronized (this) {
						wait(FIX_FINGER_INTERVAL);
					}
				} catch (InterruptedException e) {
					break;
				}
				stabilize();
				fixFinger(fingerTable.getRandomFinger());
			}
		}
	};

	private void checkBounds(long key) {
		if(0 > key || key >= keySpace)
			throw new IllegalArgumentException(String.format("key value (%s) is outside the allowable bounds [0, %s)", key, keySpace));
	}
	
	public RMINode(final int hashLength, final long nodeKey) {
		long keyspace = (1 << hashLength) - 1;
		if (nodeKey > keyspace)
			throw new IllegalArgumentException(String.format("nodeKey (%s) cannot exceed the max keyspace (%s)", nodeKey, keyspace));

		this.hashLength = hashLength;
		keySpace = 1 << hashLength;
		this.nodeKey = nodeKey;
		networkRetries = hashLength + 1;
		logger = new NodeFileLogger(nodeKey);
		fingerTable = new FingerTable(this.hashLength, this.nodeKey);
		backgroundThread.start();
	}

	/**
	 * Check if this node has left and throw an exception if it has.
	 * 
	 * @throws RemoteException
	 */
	private void checkHasNodeLeft() throws RemoteException {
		if (hasNodeLeft)
			throw new RemoteException();
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public int getHashLength() throws RemoteException {
		checkHasNodeLeft();
		return hashLength;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public long getNodeKey() throws RemoteException {
		checkHasNodeLeft();
		return nodeKey;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public NodeState getState() throws RemoteException {
		checkHasNodeLeft();
		long predecessorNodeKey;
		try {
			predecessorNodeKey = predecessor.getNodeKey();
		} catch (NullPointerException | RemoteException e) {
			predecessor = null;
			predecessorNodeKey = nodeKey * -1;
		}

		List<Long> fingers = new ArrayList<>();
		for (Finger f : fingerTable)
			try {
				fingers.add(f.getNode().getNodeKey());
			} catch (NullPointerException | RemoteException e) {
				f.setNode(null);
				fingers.add(f.getStart() * -1);
			}

		return new NodeState(getNodeKey(), predecessorNodeKey, fingers, new HashSet<Long>(nodeStorage.keySet()));
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void join(RMINodeServer fromNetwork) throws RemoteException {
		checkHasNodeLeft();
		if (fromNetwork != null) {
			fingerTable.getSuccessor().setNode(fromNetwork.findSuccessor(nodeKey));
			logger.logOutput("Joined network");
		} else {
			// the network is empty
			fingerTable.getSuccessor().setNode(this);
			logger.logOutput("Network is empty; setting successor to self");
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void leave() throws RemoteException {
		hasNodeLeft = true;
		backgroundThread.interrupt();
		logger.logOutput("Left network");
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Serializable get(long key) throws RemoteException {
		checkBounds(key);
		
		for (int i = 0; i < networkRetries; i++) {
			checkHasNodeLeft();
			try {
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey())
					return nodeStorage.get(key);
				else
					return server.get(key);
			} catch (NullPointerException | RemoteException e) {
				// some node somewhere is dead... wait a while for our fingers to
				// correct then try again
				logger.logOutput("Encountered an error during retrieval with key " + key + "; " + e.getMessage());
				logger.logOutput("Retrying...");
				try {
					Thread.sleep(FIX_FINGER_INTERVAL);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		logger.logOutput("Unable to get value with key " + key + "due to errors");
		return null;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public Serializable get(String key) throws RemoteException {
		return get(new KeyHash<String>(key, hashLength).getHash());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void put(long key, Serializable value) throws RemoteException {
		checkBounds(key);
		
		for (int i = 0; i < networkRetries; i++) {
			checkHasNodeLeft();
			try {
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey()) {
					nodeStorage.put(key, value);
					try {
						fingerTable.getSuccessor().getNode().backup(key, value);
					} catch (Throwable t) {
					}
				} else
					server.put(key, value);
			} catch (NullPointerException | RemoteException e) {
				// some node somewhere is dead... wait a while for our fingers to
				// correct then try again
				logger.logOutput("Encountered an error during insert with key " + key + "; " + e.getMessage());
				logger.logOutput("Retrying...");
				try {
					Thread.sleep(FIX_FINGER_INTERVAL);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		// tried a bunch of times and failed, throw in the towel.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void put(String key, Serializable value) throws RemoteException {
		put(new KeyHash<String>(key, hashLength).getHash(), value);
	}

	@Override
	public void backup(long key, Serializable value) throws RemoteException {
		checkHasNodeLeft();

		nodeStorage.put(key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(long key) throws RemoteException {
		checkBounds(key);

		for (int i = 0; i < networkRetries; i++) {
			checkHasNodeLeft();
			try {
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey())
					nodeStorage.remove(key);
				else
					server.delete(key);
			} catch (NullPointerException | RemoteException e) {
				// some node somewhere is dead... wait a while for our fingers to
				// correct then try again
				logger.logOutput("Encountered an error during delete with key " + key + "; " + e.getMessage());
				logger.logOutput("Retrying...");
				try {
					Thread.sleep(FIX_FINGER_INTERVAL);
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
			}
		}
		// tried a bunch of times and failed, throw in the towel.
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void delete(String key) throws RemoteException {
		delete(new KeyHash<String>(key, hashLength).getHash());
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RMINodeServer findSuccessor(long key) throws RemoteException {
		checkBounds(key);
		checkHasNodeLeft();
		
		long successorNodeKey;
		try {
			successorNodeKey = fingerTable.getSuccessor().getNode().getNodeKey();
		} catch (NullPointerException | RemoteException e) {
			fingerTable.getSuccessor().setNode(this);
			return findSuccessor(key);
		}

		if (ringRange.isInRange(false, nodeKey, key, successorNodeKey, true))
			return fingerTable.getSuccessor().getNode();

		for (Finger f : fingerTable.reverse()) {
			try {
				if (ringRange.isInRange(false, nodeKey, f.getNode().getNodeKey(), key, false))
					return f.getNode().findSuccessor(key);
			} catch (NullPointerException | RemoteException e) {
				f.setNode(f.getStart() == fingerTable.getSuccessor().getStart() ? this : null);
			}
		}

		return this;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public RMINodeServer getPredecessor() throws RemoteException {
		checkHasNodeLeft();
		return predecessor;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void checkPredecessor(RMINodeServer potentialPredecessor) throws RemoteException {
		checkHasNodeLeft();

		long potentialPredecessorNodeKey;
		try {
			potentialPredecessorNodeKey = potentialPredecessor.getNodeKey();
		} catch (NullPointerException | RemoteException e) {
			return;
		}

		try {
			if (ringRange.isInRange(false, predecessor.getNodeKey(), potentialPredecessorNodeKey, nodeKey, false)) {
				predecessor = potentialPredecessor;
				// we're only responsible for our own values, and our predecessor's
				// values. Prune out the rest.
				long predecessor_predecessorKey = potentialPredecessor.getPredecessor().getNodeKey();
				for (Long key : nodeStorage.keySet())
					if (ringRange.isInRange(false, nodeKey, key, predecessor_predecessorKey, true))
						nodeStorage.remove(key);
			}
		} catch (NullPointerException | RemoteException e) {
			predecessor = potentialPredecessor;
		}
	}

	private void fixFinger(Finger f) {
		try {
			f.setNode(findSuccessor(f.getStart()));
		} catch (RemoteException e) {
			logger.logOutput(e.getMessage());
			f.setNode(null);
		}
	}

	private void stabilize() {
		long successorNodeKey;
		RMINodeServer successor;
		try {
			successor = fingerTable.getSuccessor().getNode();
			successorNodeKey = successor.getNodeKey();
		} catch (NullPointerException | RemoteException e) {
			successor = this;
			fingerTable.getSuccessor().setNode(this);
			successorNodeKey = nodeKey;
		}

		try {
			RMINodeServer successor_predecessor = successor.getPredecessor();
			if (successor_predecessor != null && ringRange.isInRange(false, nodeKey, successor_predecessor.getNodeKey(), successorNodeKey, false))
				fingerTable.getSuccessor().setNode(successor_predecessor);
			long predecessorKey;
			try {
				predecessorKey = predecessor.getNodeKey();
			} catch (Throwable t) {
				predecessorKey = nodeKey;
			}
			// we need to forward all values we're responsible for to our new
			// successor, for them to store as a backup
			for (Long key : nodeStorage.keySet()) {
				if (ringRange.isInRange(false, predecessorKey, key, nodeKey, true)) {
					successor_predecessor.backup(key, nodeStorage.get(key));
				}
			}
		} catch (RemoteException e) {
		}

		try {
			fingerTable.getSuccessor().getNode().checkPredecessor(this);
		} catch (RemoteException e) {
			fingerTable.getSuccessor().setNode(this);
		}
	}
}
