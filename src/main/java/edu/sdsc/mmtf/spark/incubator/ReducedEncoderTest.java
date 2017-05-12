package edu.sdsc.mmtf.spark.incubator;

import static org.junit.Assert.*;

import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;

public class ReducedEncoderTest {

	@Test
	public void test(StructureDataInterface full) {
		StructureDataInterface red = ReducedEncoder.getReduced(full);
		String id = full.getStructureId();
		
		assertEquals(id + " NumEntities: ",full.getNumEntities(), red.getNumEntities());
		assertEquals(id + " NumChains: ", full.getNumChains(), red.getNumChains());
		assertEquals(id + " NumModels: ", full.getNumModels(), red.getNumModels());
		assertArrayEquals(id + " ChainIds: ", full.getChainIds(), red.getChainIds());
//		assertArrayEquals(id + " ChainsPerModel: ",full.getChainsPerModel(), red.getChainsPerModel());
//		assertEquals(id + " NumGroups: ", full.getNumGroups(), red.getNumGroups()); // can be less (-water)
//		assertArrayEquals(id + " GroupsPerChain: ", full.getGroupsPerChain(), red.getGroupsPerChain());



	}

}
