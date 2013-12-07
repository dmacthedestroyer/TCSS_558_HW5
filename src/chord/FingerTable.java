package chord;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Random;

/**
 * Defines the FingerTable used by RMINodes.
 * The FingerTable has the list of fingers for one node.
 * 
 * @author dmac
 *
 */

public class FingerTable implements Iterable<Finger> {

	/**
	 * Random number generator for use in the getRandomFinger method
	 */
	private static Random random = new Random();

	private List<Finger> fingers;

	/**
	 * FingerTable Constructor.
	 * Populates the finger table with the nodeKeys of each finger.
	 * 
	 * @param forNode
	 * @throws RemoteException
	 */
	public FingerTable(final int hashLength, final long nodeKey) {
		fingers = new ArrayList<Finger>();
		long maxValue = (long) Math.pow(2, hashLength);
		for (int i = 0; i < hashLength; i++)
			fingers.add(new Finger((long) ((nodeKey + Math.pow(2, i)) % maxValue)));
	}

	/**
	 * Returns the first finger in the finger table (the successor).
	 * 
	 * @return
	 */
	public Finger getSuccessor() {
		if (fingers.size() == 0)
			return null;
		return fingers.get(0);
	}

	/**
	 * Returns a random finger that is not the successor.
	 * 
	 * @return
	 */
	public Finger getRandomFinger() {
		return fingers.get(random.nextInt(fingers.size()-1)+1);
	}

	public Iterable<Finger> reverse() {
		return new ReverseIterator<Finger>(fingers);
	}

	@Override
	public Iterator<Finger> iterator() {
		return fingers.iterator();
	}

	/**
	 * The finger is conveted to a human-readable format.
	 * This is used for logging. 
	 */
	@Override
	public String toString() {
		String s = "";
		for(Finger f: this) {
			s += f.getStart() + ":";
			try{
			s += f.getNode().getNodeKey();
			} catch(NullPointerException | RemoteException e){
				s += "-";
			}
			s += " ";
		}
		
		return s;
	}
	
	/**
	 * An iterator for lists that allows the programmer to traverse the list in
	 * reverse order.
	 * 
	 * @param <T>
	 */
	class ReverseIterator<T> implements Iterable<T> {
		private ListIterator<T> listIterator;

		public ReverseIterator(List<T> wrappedList) {
			this.listIterator = wrappedList.listIterator(wrappedList.size());
		}

		public Iterator<T> iterator() {
			return new Iterator<T>() {

				public boolean hasNext() {
					return listIterator.hasPrevious();
				}

				public T next() {
					return listIterator.previous();
				}

				public void remove() {
					listIterator.remove();
				}
			};
		}
	}
}
