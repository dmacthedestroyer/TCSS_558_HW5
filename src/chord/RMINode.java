package chord;

import java.io.Serializable;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

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
				forwardValuesForBackup();
			}
		}
	};

	private void checkBounds(long key) {
		if (0 > key || key >= keySpace)
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
			RMINodeServer successor = fromNetwork.findSuccessor(nodeKey);
			fingerTable.getSuccessor().setNode(successor);
			successor.checkPredecessor(this);

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
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public Serializable get(final long key) throws RemoteException, NetworkHosedException {
		checkBounds(key);
		return new Action<Serializable>() {

			@Override
			Serializable execute() throws RemoteException, NetworkHosedException {
				checkHasNodeLeft();
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey())
					return nodeStorage.get(key);
				else
					return server.get(key);
			}
		}.getResult();
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public Serializable get(String key) throws RemoteException, NetworkHosedException {
		return get(new KeyHash<String>(key, hashLength).getHash());
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public void put(final long key, final Serializable value) throws RemoteException, NetworkHosedException {
		checkBounds(key);
		new Action<Void>() {

			@Override
			Void execute() throws RemoteException, NetworkHosedException {
				checkHasNodeLeft();
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey()) {
					logger.logOutput("Adding value with key " + key);
					nodeStorage.put(key, value);
					fingerTable.getSuccessor().getNode().putBackup(key, value);
				} else
					server.put(key, value);

				return null;
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public void put(String key, Serializable value) throws RemoteException, NetworkHosedException {
		put(new KeyHash<String>(key, hashLength).getHash(), value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void putBackup(long key, Serializable value) throws RemoteException {
		checkHasNodeLeft();
		nodeStorage.put(key, value);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void removeBackup(long key) throws RemoteException {
		nodeStorage.remove(key);
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public void delete(final long key) throws RemoteException, NetworkHosedException {
		checkBounds(key);
		new Action<Void>() {

			@Override
			Void execute() throws RemoteException, NetworkHosedException {
				checkHasNodeLeft();
				RMINodeServer server = findSuccessor(key);
				if (nodeKey == server.getNodeKey()) {
					nodeStorage.remove(key);
					fingerTable.getSuccessor().getNode().removeBackup(key);
				} else
					server.delete(key);

				return null;
			}
		};
	}

	/**
	 * {@inheritDoc}
	 * 
	 * @throws NetworkHosedException
	 */
	@Override
	public void delete(String key) throws RemoteException, NetworkHosedException {
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
			final RMINodeServer successor_predecessor = successor.getPredecessor();
			if (successor_predecessor != null && ringRange.isInRange(false, nodeKey, successor_predecessor.getNodeKey(), successorNodeKey, false)) {
				fingerTable.getSuccessor().setNode(successor_predecessor);
			}
		} catch (RemoteException e) {
		}

		try {
			fingerTable.getSuccessor().getNode().checkPredecessor(this);
		} catch (RemoteException e) {
			fingerTable.getSuccessor().setNode(this);
		}
	}

	/**
	 * forward all backed up values we have to our predecessor
	 */
	private void forwardValuesForBackup() {
		new Thread(new Runnable() {

			@Override
			public void run() {
				try {
					new Action<Void>() {
						@Override
						Void execute() throws RemoteException, NetworkHosedException {
							long predecessorKey = predecessor.getNodeKey();
							long predecessorPredecessorKey = predecessor.getPredecessor().getNodeKey();
							for (Entry<Long, Serializable> pair : nodeStorage.entrySet()) {
								//send our predecessor its values that we have on backup
								if (ringRange.isInRange(false, predecessorPredecessorKey, pair.getKey(), predecessorKey, true))
									predecessor.putBackup(pair.getKey(), pair.getValue());
								//send our successor our values for them to back up
								if (ringRange.isInRange(false, predecessorKey, pair.getKey(), nodeKey, true))
									fingerTable.getSuccessor().getNode().putBackup(pair.getKey(), nodeStorage.get(pair.getKey()));
								//trim out any excess values from old predecessors
								if (!ringRange.isInRange(false, predecessorPredecessorKey, pair.getKey(), nodeKey, true))
									nodeStorage.remove(pair.getKey());
							}
							return null;
						}
					};
				} catch (NetworkHosedException e) {
				}
			}
		}).start();
	}

	private abstract class Action<T> {
		private T result;

		abstract T execute() throws RemoteException, NetworkHosedException;

		public Action() throws NetworkHosedException {
			Exception lastException = null;
			for (int i = 0; i < networkRetries; i++)
				try {
					result = execute();
					return;
				} catch (NullPointerException | RemoteException e) {
					lastException = e;
					try {
						Thread.sleep(FIX_FINGER_INTERVAL);
					} catch (InterruptedException e1) {
					}
				}

			throw new NetworkHosedException("The network is hosed", lastException);
		}

		public T getResult() {
			return result;
		}
	}
}
