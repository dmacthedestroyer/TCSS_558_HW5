package util;
import java.util.HashSet;
import java.util.Random;

/**
 * Generates a unique set of random values between 0 and the specified max value
 * @author dmac
 *
 */
public class RandomNumberSet {
	
	private long maxValue;
	
	private HashSet<Long> assignedValues;
	
	/**
	 * random number generator.  I made it static because, well, why not?
	 */
	private static final Random random = new Random();
	
	/**
	 * this is a constructor.  You know what to do.
	 * @param maxValue the max value that can be generated
	 */
	public RandomNumberSet(long maxValue) {
		assignedValues = new HashSet<Long>();
		this.maxValue = maxValue;
	}
	
	/**
	 * generate the next random value
	 * @return
	 */
	public long next(){
		return validateValue(Math.abs(random.nextLong()) % maxValue);
	}
	
	/**
	 * ensures the given value has not already been generated, or increments until a unique value is generated
	 * @param value
	 * @return
	 */
	private long validateValue(final long value) {
		if(assignedValues.contains(value))
			return validateValue((value + 1) % maxValue);
		
		assignedValues.add(value);
		return value;
	}
}
