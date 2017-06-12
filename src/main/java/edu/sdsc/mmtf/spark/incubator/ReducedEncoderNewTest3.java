package edu.sdsc.mmtf.spark.incubator;


import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Test;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.encoder.ReducedEncoder;

import edu.sdsc.mmtf.spark.io.MmtfReader;
import scala.Tuple3;

public class ReducedEncoderNewTest3 {

	@Test
	public void test() {
		String path = System.getProperty("MMTF_FULL");
		if (path == null) {
			System.err.println("Environment variable for Hadoop sequence file has not been set");
			System.exit(-1);
		}

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName(ReducedEncoderNewTest3.class.getSimpleName());
		JavaSparkContext sc = new JavaSparkContext(conf);

		// Failure cases
		// deuterated: 1LZN, 2INQ, 2ZYE, 3KYW,3Q3L, 5DNP, 5KWF, 1GKT, 2WYX
		// 2G10, 2ICY ?? problem with groupIds

		//    List<String> pdbIds = Arrays.asList("1PLX","1IGT","1LPV","1MSH","1R9V","4CK4","4P3R");
		List<String> exclude = Arrays.asList("1LZN","2INQ","2G10","2ICY","2ZYE","3KYW","3Q3L","5DPN","5KWF","1GKT","2WYX");

		JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.readSequenceFile(path, sc);
		//	    JavaPairRDD<String, StructureDataInterface> pdb = MmtfReader.downloadMmtfFiles(pdbIds, sc).cache();	    

		pdb = pdb.filter(t -> !exclude.contains(t._1));
		pdb.map(t -> new Tuple3<String, StructureDataInterface, StructureDataInterface>
		(t._1, t._2, ReducedEncoderNew.getReduced(t._2)))
//				    (t._1, t._2, ReducedEncoder.getReduced(t._2)))
		.foreach(v -> compareFullVsReduced(v._1(), v._2(), v._3()));

		sc.close();
	}

	// check
	// spacegroup for NMR structure P1
	// mmtf producer
	//
	private static void compareFullVsReduced(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		compareMetaData(structureId, full, reduced);
		compareAtomData(structureId, full, reduced);
		compareUnitCellData(structureId, full, reduced);
		compareBioAssemblyData(structureId, full, reduced);
		compareChainData(structureId, full, reduced);
		compareGroupData(structureId, full, reduced);
		compareInterBondData(structureId, full, reduced);
	}

	private static void compareMetaData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		assertEquals(structureId + ":Entities", full.getNumEntities(), reduced.getNumEntities());
		assertEquals(structureId + ":Models", full.getNumModels(), reduced.getNumModels());
		assertEquals(structureId + ":DepositionDate", full.getDepositionDate(), reduced.getDepositionDate());
		assertEquals(structureId + ":ReleaseDate", full.getReleaseDate(), reduced.getReleaseDate());
		// TODO null for r	    assertEquals(structureId + "MmtfProducer", full.getMmtfProducer(), reduced.getMmtfProducer());
		assertEquals(structureId + ":MmtfVersion", full.getMmtfVersion(), reduced.getMmtfVersion());
		assertEquals(structureId + ":Resolution", full.getResolution(), reduced.getResolution(), 0.001);
		assertEquals(structureId + ":Rfree", full.getRfree(), reduced.getRfree(), 0.001);
		assertEquals(structureId + ":Rwork", full.getRwork(), reduced.getRwork(), 0.001);
		assertEquals(structureId + ":StructureId", full.getStructureId(), reduced.getStructureId());
		assertArrayEquals(structureId + ":ExperimentalMethods", full.getExperimentalMethods(), reduced.getExperimentalMethods());
	}

	private static void compareAtomData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getxCoords().length);
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getyCoords().length);
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getzCoords().length);
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getbFactors().length);
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getAltLocIds().length);
		assertEquals(structureId + ":NumAtoms", reduced.getNumAtoms(), reduced.getAtomIds().length);
	}

	private static void compareBioAssemblyData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		assertEquals(structureId + ":NumBioassemblies", full.getNumBioassemblies(), reduced.getNumBioassemblies());

		for (int i = 0; i < full.getNumBioassemblies(); i++) {
			assertEquals(structureId + ":BioassemblyName", full.getBioassemblyName(i), reduced.getBioassemblyName(i));
			assertEquals(structureId + ":NumTransInBioassembly", full.getNumTransInBioassembly(i), reduced.getNumTransInBioassembly(i));

			for (int j = 0; j < full.getNumTransInBioassembly(i); j++) {
				assertArrayEquals(structureId + ":MatrixForTransform", full.getMatrixForTransform(i, j), reduced.getMatrixForTransform(i, j), 0.00001);
				assertArrayEquals(structureId + ":ChainIndexListForTransform", full.getChainIndexListForTransform(i, j), reduced.getChainIndexListForTransform(i, j));
			}
		}
	}

	private static void compareUnitCellData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		// TODO missing, null for NMR 
		if (full.getUnitCell() != null || reduced.getUnitCell() != null) {
			assertEquals(structureId + ":SpaceGroup", full.getSpaceGroup(), reduced.getSpaceGroup());
			assertArrayEquals(structureId + ":UnitCell", full.getUnitCell(), reduced.getUnitCell(), 0.0001f);
			if (full.getNcsOperatorList() != null) {
				for (int i = 0; i < full.getNcsOperatorList().length; i++) {
					assertArrayEquals(structureId + ":NcsOperatorList", full.getNcsOperatorList()[i], reduced.getNcsOperatorList()[i], 0.0001);
				}
			}
		}
	}


	private static void compareChainData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		assertArrayEquals(structureId + "ChainNames", full.getChainNames(), reduced.getChainNames());
		assertArrayEquals(structureId + "ChainIds", full.getChainIds(), reduced.getChainIds());
		assertArrayEquals(structureId + "ChainsPerModel", full.getChainsPerModel(), reduced.getChainsPerModel());
	}

	private static void compareGroupData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		List<String> traceNames = Arrays.asList("CA","P");

		// this checks only first model, since the data are interleaved for multiple models, they cannot be compared directly
		int n = reduced.getNumGroups()/reduced.getNumModels();
		assertArrayEquals(structureId + ":GroupIds", Arrays.copyOf(full.getGroupIds(),n), Arrays.copyOf(reduced.getGroupIds(),n));
		assertTrue(structureId + ":GroupSequenceIndices", full.getGroupSequenceIndices().length >= reduced.getGroupSequenceIndices().length);
		assertArrayEquals(structureId + ":GroupSequenceIndices", Arrays.copyOf(full.getGroupSequenceIndices(),n), Arrays.copyOf(reduced.getGroupSequenceIndices(),n));
		assertEquals(structureId + ":NumGroups", full.getNumGroups(), full.getGroupSequenceIndices().length);
		assertEquals(structureId + ":NumGroups", reduced.getNumGroups(), reduced.getGroupSequenceIndices().length);
		assertEquals(structureId + ":NumGroups", full.getNumGroups(), full.getGroupTypeIndices().length);
		assertEquals(structureId + ":NumGroups", reduced.getNumGroups(), reduced.getGroupTypeIndices().length);

		for (int i = 0; i < reduced.getNumGroups()/reduced.getNumModels(); i++) {
			int fId = full.getGroupTypeIndices()[i];
			int rId = reduced.getGroupTypeIndices()[i];

			assertEquals(structureId + ":GroupChemCompType", full.getGroupChemCompType(fId), reduced.getGroupChemCompType(rId));
			assertEquals(structureId + ":GroupName", full.getGroupName(fId), reduced.getGroupName(rId));
			assertEquals(structureId + ":GroupSingleLetterCode", full.getGroupSingleLetterCode(fId), reduced.getGroupSingleLetterCode(rId));

			// there will be fewer atoms and bonds per group for peptide and nucleotide groups
			assertTrue(structureId + ":NumAtomsInGroup", full.getNumAtomsInGroup(fId) >= reduced.getNumAtomsInGroup(rId));
			assertTrue(structureId + ":GroupAtomCharges", full.getGroupAtomCharges(fId).length >= reduced.getGroupAtomCharges(rId).length);
			assertTrue(structureId + ":GroupAtomNames", full.getGroupAtomNames(fId).length >= reduced.getGroupAtomNames(rId).length);
			assertTrue(structureId + ":GroupElementNames", full.getGroupElementNames(fId).length >= reduced.getGroupElementNames(rId).length);

			// if number of atoms per group are the same, then these data must be identical
			if (full.getNumAtomsInGroup(fId) == reduced.getNumAtomsInGroup(rId)) {
				assertArrayEquals(structureId + ":GroupAtomCharges", full.getGroupAtomCharges(fId), reduced.getGroupAtomCharges(rId));
				assertArrayEquals(structureId + ":GroupAtomNames", full.getGroupAtomNames(fId), reduced.getGroupAtomNames(rId));
			} else {
				assertTrue(structureId + ":GroupAtomNames", traceNames.containsAll(Arrays.asList(reduced.getGroupAtomNames(rId))));
			}

			assertTrue(structureId + ":GroupBondIndices", full.getGroupBondIndices(fId).length >= reduced.getGroupBondIndices(rId).length);
			assertTrue(structureId + ":GroupBondOrders", full.getGroupBondOrders(fId).length >= reduced.getGroupBondOrders(rId).length);

			// for all other groups, the bond info should be identical
			if (full.getGroupBondIndices(fId).length == reduced.getGroupBondIndices(rId).length) {
				assertArrayEquals(structureId + ":GroupBondIndices", full.getGroupBondIndices(fId), reduced.getGroupBondIndices(rId));
				for (int bo: reduced.getGroupBondOrders(rId)) {
					assertTrue(structureId + ":GroupBondOrders", bo >0 && bo < 5);
				}
				assertArrayEquals(structureId + ":GroupBondOrders", full.getGroupBondOrders(fId), reduced.getGroupBondOrders(rId));
			} 
		}
	}

	private static void compareInterBondData(String structureId, StructureDataInterface full, StructureDataInterface reduced) {
		assertTrue(structureId + ":InterGroupBondIndices", full.getInterGroupBondIndices().length >= reduced.getInterGroupBondIndices().length);
		assertTrue(structureId + ":InterGroupBondOrder", full.getInterGroupBondOrders().length >= reduced.getInterGroupBondOrders().length);
		for (int bo: reduced.getInterGroupBondOrders()) {
			assertTrue(structureId + ":InterGroupBondOrders", bo >0 && bo < 5);
		}
	}
}
