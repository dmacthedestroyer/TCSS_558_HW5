package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import org.junit.Before;
import org.junit.Test;

import chord.Finger;
import chord.FingerTable;

public class FingerTableTest {

	FingerTable table;

	@Before
	public void setUp() throws Exception {
		table = new FingerTable(2, 0);
	}

	@Test
	public void testGetSuccessor() {
		Finger successor = table.getSuccessor();
		assertNotEquals(successor, null);
	}

	@Test
	public void testGetRandomFinger() {
		Finger randomFinger = table.getRandomFinger();
		assertEquals(null, randomFinger.getNode());
	}

}
