package edu.sdsc.mmtf.spark.incubator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.rcsb.mmtf.api.StructureDataInterface;
import org.rcsb.mmtf.dataholders.Entity;

import scala.Tuple2;

/**
 * Convert a full format of the file to a reduced format.
 * @author Anthony Bradley
 *
 */
public class StructureToPolymerType implements PairFlatMapFunction<Tuple2<String,StructureDataInterface>,String, String> {
	private static final long serialVersionUID = -3348372120358649240L;

	@Override
	public Iterator<Tuple2<String, String>> call(Tuple2<String, StructureDataInterface> t) throws Exception {
		return getReduced(t._2).iterator();
	}


	/**
	 * Get the reduced form of the input {@link StructureDataInterface}.
	 * @param structureDataInterface the input {@link StructureDataInterface} 
	 * @return the reduced form of the {@link StructureDataInterface} as another {@link StructureDataInterface}
	 */
	public static List<Tuple2<String, String>> getReduced(StructureDataInterface structureDataInterface) {
		int numChains = structureDataInterface.getChainsPerModel()[0];
		List<Tuple2<String, String>> chainList = new ArrayList<>(numChains);

		for (int j=0; j<numChains; j++){			
			Entity entity = getEntityInfo(structureDataInterface, j);
			chainList.add(new Tuple2<String,String>(structureDataInterface.getStructureId()+structureDataInterface.getChainIds()[j], entity.getType()));
		}

		return chainList;
	}

	/**
	 * Returns entity information for the chain specified by the chain index.
	 * @param structureDataInterface
	 * @param chainIndex
	 * @return
	 */
	private static Entity getEntityInfo(StructureDataInterface structureDataInterface, int chainIndex) {
		Entity entity = new Entity();

		for (int entityInd = 0; entityInd < structureDataInterface.getNumEntities(); entityInd++) {

			for (int chainInd: structureDataInterface.getEntityChainIndexList(entityInd)) {
				if (chainInd == chainIndex) {
					entity.setChainIndexList(new int[]{0}); // new chain index is zero, since we extract a single chain
					entity.setDescription(structureDataInterface.getEntityDescription(entityInd));
					entity.setSequence(structureDataInterface.getEntitySequence(entityInd));
					entity.setType(structureDataInterface.getEntityType(entityInd));
					return entity;
				}
			}
		}
		return entity;
	}

}