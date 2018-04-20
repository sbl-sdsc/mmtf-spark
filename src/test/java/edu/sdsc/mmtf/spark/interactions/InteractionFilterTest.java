package edu.sdsc.mmtf.spark.interactions;

import static org.junit.Assert.*;

import org.junit.Test;

import edu.sdsc.mmtf.spark.interactions.InteractionFilter;

public class InteractionFilterTest {

	@Test
	public void test1() {
		InteractionFilter filter = new InteractionFilter();
		filter.setQueryGroups(true, "HOH","ZN");
		
		assertEquals(true, filter.isQueryGroup("ZN"));
		assertEquals(true, filter.isQueryGroup("HOH"));
		assertEquals(false, filter.isQueryGroup("MN"));
	}
	
	@Test
	public void test2() {
		InteractionFilter filter = new InteractionFilter();
		filter.setQueryGroups(false, "HOH","ZN");
		
		assertEquals(false, filter.isQueryGroup("ZN"));
		assertEquals(false, filter.isQueryGroup("HOH"));
		assertEquals(true, filter.isQueryGroup("MN"));
	}
	
	@Test
	public void test3() {
		InteractionFilter filter = new InteractionFilter();
		filter.setTargetGroups(true, "HOH","ZN");
		
		assertEquals(true, filter.isTargetGroup("ZN"));
		assertEquals(true, filter.isTargetGroup("HOH"));
		assertEquals(false, filter.isTargetGroup("MN"));
	}
	
	@Test
	public void test4() {
		InteractionFilter filter = new InteractionFilter();
		filter.setTargetGroups(false, "HOH","ZN");
		
		assertEquals(false, filter.isTargetGroup("ZN"));
		assertEquals(false, filter.isTargetGroup("HOH"));
		assertEquals(true, filter.isTargetGroup("MN"));
	}
	@Test
	public void test5() {
		InteractionFilter filter = new InteractionFilter();
		
		assertEquals(true, filter.isQueryGroup("ZN"));
		assertEquals(true, filter.isTargetGroup("ZN"));
		assertEquals(true, !filter.isProhibitedTargetGroup("ZN"));
	}
	
	@Test
	public void test6() {
		InteractionFilter filter = new InteractionFilter();
		filter.setQueryElements(true, "N","O");
		
		assertEquals(true, filter.isQueryElement("O"));
		assertEquals(true, filter.isQueryElement("N"));
		assertEquals(false, filter.isQueryElement("S"));
	}
	
	@Test
	public void test7() {
		InteractionFilter filter = new InteractionFilter();
		filter.setQueryElements(false, "N","O");
		
		assertEquals(false, filter.isQueryElement("N"));
		assertEquals(false, filter.isQueryElement("O"));
		assertEquals(true, filter.isQueryElement("S"));
	}
	
	@Test
	public void test8() {
		InteractionFilter filter = new InteractionFilter();
		filter.setTargetElements(true, "N","O");
		
		assertEquals(true, filter.isTargetElement("O"));
		assertEquals(true, filter.isTargetElement("N"));
		assertEquals(false, filter.isTargetElement("S"));
	}
	
	@Test
	public void test9() {
		InteractionFilter filter = new InteractionFilter();
		filter.setTargetElements(false, "N","O");
		
		assertEquals(false, filter.isTargetElement("N"));
		assertEquals(false, filter.isTargetElement("O"));
		assertEquals(true, filter.isTargetElement("S"));
	}
	
    @Test
    public void test10() {
        InteractionFilter filter = new InteractionFilter();
        filter.setQueryAtomNames(true, "CA", "CB");
        
        assertEquals(false, filter.isQueryAtomName("C"));
        assertEquals(false, filter.isQueryAtomName("CG"));
        assertEquals(true, filter.isQueryAtomName("CA"));
        assertEquals(true, filter.isQueryAtomName("CB"));
    }

    @Test
    public void test11() {
        InteractionFilter filter = new InteractionFilter();
        filter.setTargetAtomNames(true, "CA", "CB");

        assertEquals(false, filter.isTargetAtomName("C"));
        assertEquals(false, filter.isTargetAtomName("CG"));
        assertEquals(true, filter.isTargetAtomName("CA"));
        assertEquals(true, filter.isTargetAtomName("CB"));
    }
}
