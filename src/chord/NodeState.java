package chord;

import java.io.Serializable;
import java.util.List;
import java.util.Set;

/**
 * Represents the state of a node in the Chord network. Includes information on
 * the key, predecessor, fingers and number of values being stored by this node
 * 
 * @author dmac
 * 
 */
public class NodeState implements Serializable {

	private long key, predecessor;
	private List<Long> fingers;
	private Set<Long> valueKeys;

	public NodeState(long key, long predecessor, List<Long> fingers, Set<Long> valueKeys) {
		this.key = key;
		this.predecessor = predecessor;
		this.fingers = fingers;
		this.valueKeys = valueKeys;
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

	public Set<Long> getValueKeys() {
		return valueKeys;
	}
}
