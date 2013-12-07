package chord;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface for returning the current state of a node in a Chord network
 * @author dmac
 *
 */
public interface RMINodeState extends Remote {
	/**
	 * returns a representation of the internal state of a node, or throws a RemoteException if this node is offline
	 * @return
	 * @throws RemoteException
	 */
	NodeState getState() throws RemoteException;
}
