package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import chord.RingRange;

public class RingRangeTest {

	RingRange ringRange;
	
	@Before
	public void setup() {
		ringRange = new RingRange();
	}
	
	@Test
	public void testInsideBounds() {
		assertTrue("0 <= 1 <= 2", ringRange.isInRange(true, 0, 1, 2, true));
		assertTrue("0 <= 1 < 2", ringRange.isInRange(true, 0, 1, 2, false));
		assertTrue("0 < 1 <= 2", ringRange.isInRange(false, 0, 1, 2, true));
		assertTrue("0 < 1 < 2", ringRange.isInRange(false, 0, 1, 2, false));
	}

	@Test
	public void testLeftBound() {
		assertTrue("0 <= 0 <= 3", ringRange.isInRange(true, 0, 0, 3, true));
		assertFalse("0 < 0 <= 3", ringRange.isInRange(false, 0, 0, 3, true));
	}

	@Test
	public void testRightBound() {
		assertTrue("0 <= 3 <= 3", ringRange.isInRange(true, 0, 3, 3, true));
		assertFalse("0 <= 3 < 3", ringRange.isInRange(true, 0, 3, 3, false));
	}

	@Test
	public void testCrossRingInsideBounds() {
		assertTrue("7 <= 0 <= 1", ringRange.isInRange(true, 7, 0, 1, true));
		assertTrue("7 <= 0 < 1", ringRange.isInRange(true, 7, 0, 1, false));
		assertTrue("7 < 0 <= 1", ringRange.isInRange(false, 7, 0, 1, true));
		assertTrue("7 < 0 < 1", ringRange.isInRange(false, 7, 0, 1, false));
	}

	@Test
	public void testCrossRingLeftBound() {
		assertTrue("7 <= 7 <= 1", ringRange.isInRange(true, 7, 7, 1, true));
		assertTrue("7 <= 7 < 1", ringRange.isInRange(true, 7, 7, 1, false));
		assertFalse("7 < 7 <= 1", ringRange.isInRange(false, 7, 7, 1, true));
		assertFalse("7 < 7 < 1", ringRange.isInRange(false, 7, 7, 1, false));
	}

	@Test
	public void testCrossRingRightBound() {
		assertTrue("7 <= 1 <= 1", ringRange.isInRange(true, 7, 1, 1, true));
		assertFalse("7 <= 1 < 1", ringRange.isInRange(true, 7, 1, 1, false));
		assertTrue("7 < 1 <= 1", ringRange.isInRange(false, 7, 1, 1, true));
		assertFalse("7 < 1 < 1", ringRange.isInRange(false, 7, 1, 1, false));
	}

	@Test
	public void testLeftEqualRight() {
		assertTrue("0 <= 4 <= 0", ringRange.isInRange(true, 0, 4, 0, true));
		assertTrue("0 <= 4 < 0", ringRange.isInRange(true, 0, 4, 0, false));
		assertTrue("0 < 4 <= 0", ringRange.isInRange(false, 0, 4, 0, true));
		assertTrue("0 < 4 < 0", ringRange.isInRange(false, 0, 4, 0, false));

		assertTrue("0 <= 0 <= 0", ringRange.isInRange(true, 0, 0, 0, true));
		assertTrue("0 <= 0 < 0", ringRange.isInRange(true, 0, 0, 0, false));
		assertTrue("0 < 0 <= 0", ringRange.isInRange(false, 0, 0, 0, true));
		assertFalse("0 < 0 < 0", ringRange.isInRange(false, 0, 0, 0, false));
	}
}