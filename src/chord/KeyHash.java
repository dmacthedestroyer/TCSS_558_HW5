package chord;
/**
 * A class to properly hash node keys for the network.
 * 
 * @author dmac
 *
 * @param <T>
 */

public class KeyHash<T> {
	private final T key;
	
	private final long hash;
	
	/**
	 * Takes the key of the node (T) and the m value (bitness).
	 * Sets the hash.
	 * 
	 * @param key
	 * @param bitness
	 */
	public KeyHash(T key, int bitness){
		this.key = key;
		this.hash = (long) (Math.abs(key.hashCode()) % Math.pow(2, bitness));
	}
	
	/**
	 * Gets the key value.
	 * @return
	 */
	public T getKey() { return key; }
	
	/**
	 * Gets the hash.
	 * @return
	 */
	public long getHash() { return hash; }
}
