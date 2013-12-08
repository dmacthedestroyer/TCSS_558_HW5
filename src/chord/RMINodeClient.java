package chord;
import java.io.Serializable;
import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * The RMINodeClient exposes methods that a client can use to access the Chord network

 * @author dmac
 */
public interface RMINodeClient extends Remote {
	
	/**
	 * Gets the value stored for the given key, or null if no value exists
	 * @param key
	 * @return
	 * @throws RemoteException
	 */
	public Serializable get(String key) throws RemoteException, NetworkHosedException;
	
	/**
	 * Sets the value for the given key to the given value
	 * @param key
	 * @param value
	 * @throws RemoteException
	 */
	public void put(String key, Serializable value) throws RemoteException, NetworkHosedException;
	
	/**
	 * Removes the value at the given key
	 * @param key
	 * @throws RemoteException
	 */
	public void delete(String key) throws RemoteException, NetworkHosedException;
}
