package chord;
/**
 * Defines the Finger class used by FingerTable. 
 * 
 * @author dmac
 *
 */
public class Finger {

	private final long startKey;
	
	private RMINodeServer node;
	
	/**
	 * This is the key that defines the range of the finger.
	 * 
	 * @param startKey
	 */
	public Finger(final long startKey){
		this.startKey = startKey;
	}
	/**
	 * Gets the start key for the node.
	 * @return
	 */
	public long getStart(){
		return startKey;
	}

	/**
	 * Gets the node object.
	 * @return
	 */
	public RMINodeServer getNode() {
		return node;
	}

	/**
	 * Sets the node object.
	 * @param node
	 */
	public void setNode(final RMINodeServer node){
		this.node = node;
	}
}
