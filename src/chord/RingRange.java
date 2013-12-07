package chord;

/**
 * This class encapsulates the logic for determining if a given value is within
 * a specified interval in a ring topology, handling complications like whether
 * the start and end values of the interval span the beginning of the ring
 * 
 * @author dmac
 * 
 */
public class RingRange {
	/**
	 * Determines if x is between left and right, with the specified closedness or
	 * openness of the bounds
	 * 
	 * @param leftInclusive
	 * @param left
	 * @param x
	 * @param right
	 * @param rightInclusive
	 * @return
	 */
	public boolean isInRange(boolean leftInclusive, long left, long x, long right, boolean rightInclusive) {
		// if left and right are the same, then the whole ring is within the
		// interval (except for the bound, if both sides are exclusive)
		if (left == right && (leftInclusive || rightInclusive || x != left))
			return true;

		boolean xleft = (leftInclusive && x == left) || x > left;
		boolean xright = (rightInclusive && x == right) || x < right;

		if (left <= right)
			return xleft && xright;

		return xleft || xright;
	}
}
