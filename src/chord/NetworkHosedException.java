package chord;

public class NetworkHosedException extends Exception {
	/**
	 * An exception that is thrown when it has been determined that the network is
	 * damaged beyond repair.
	 * 
	 * @author dmac
	 * 
	 */
	public NetworkHosedException(String message) {
		super(message);
	}
}
