package chord;

import java.io.Serializable;
import java.util.List;

/**
 * Represents the state of a node in the Chord network. Includes information on
 * the key, predecessor, fingers and number of values being stored by this node
 * 
 * @author dmac
 * 
 */
public class NodeState implements Serializable {

	private long key, predecessor, valueCount;
	private List<Long> fingers;

	public NodeState(long key, long predecessor, List<Long> fingers, long valueCount) {
		this.key = key;
		this.predecessor = predecessor;
		this.fingers = fingers;
		this.valueCount = valueCount;
	}

	public long getKey() {
		return key;
	}

	public long getPredecessor() {
		return predecessor;
	}

	public List<Long> getFingers() {
		return fingers;
	}

	public long getValueCount() {
		return valueCount;
	}
}
