package chord;
import java.io.Serializable;
import java.rmi.RemoteException;

/** 
 * The RMINodeServer exposes methods that a node can use to interact with other nodes via RMI.
 */
public interface RMINodeServer extends RMINodeClient {
	/**
	 * entry point for joining an existing chord network
	 * @param fromNetwork
	 * @throws RemoteException
	 */
	public void join(RMINodeServer fromNetwork) throws RemoteException;
	
	/**
	 * have this node leave the chord network
	 * @throws RemoteException
	 */
	public void leave() throws RemoteException;
	
	/**
	 * returns the log(base2) of the total number of nodes allowed in the network
	 * @return
	 * @throws RemoteException
	 */
	public int getHashLength() throws RemoteException;
	
	/**
	 * returns the id of this node
	 * @return
	 * @throws RemoteException
	 */
	public long getNodeKey() throws RemoteException;

	/**
	 * returns the value stored at the given key in the network
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public Serializable get(long key) throws RemoteException;
	
	/**
	 * sets the given value at the given key in the network
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	public void put(long key, Serializable value) throws RemoteException;
	
	/**
	 * removes the value at the given key in the network
	 * @param key
	 * @throws RemoteException
	 */
	public void delete(long key) throws RemoteException;
	
	/**
	 * Finds the successor node for the given key
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public RMINodeServer findSuccessor(long key) throws RemoteException;
	
	/**
	 * Finds the predecessor node for the given key
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public RMINodeServer getPredecessor() throws RemoteException;
	
	/**
	 * Check whether the specified node should be your predecessor
	 * @param potentialPredecessor the node which should be checked against this node's current predecessor
	 * @return
	 * @throws RemoteException
	 */
	public void checkPredecessor(RMINodeServer potentialPredecessor) throws RemoteException;
	
	/**
	 * Notifies this node that the given leavingNode is exiting the network, so all references to it should be updated
	 * @param leavingNode
	 * @throws RemoteException
	 */
	public void nodeLeaving(long leavingNodeKey) throws RemoteException;
}
