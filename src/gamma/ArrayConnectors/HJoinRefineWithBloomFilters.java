/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma.ArrayConnectors;

import basicConnector.Connector;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import paralleljoin.BFilter;
import paralleljoin.Bloom;
import paralleljoin.HJoin;
import paralleljoin.Print;
import paralleljoin.ReadRelation;

/**
 *
 * @author floydgb
 */
public class HJoinRefineWithBloomFilters {
    public void runHJoinRefineWithBloomFilters(String r1name, String r2name, int jk1, int jk2
    ) throws Exception {
        System.out.println("Joining " + r1name + " with " + r2name);
        ThreadList.init();

        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1);
        Relation rel1 = Relation.GetRelationByName(r1name);
        c1.setRelation(rel1);

        Connector bloomTupleConnectorOut = new Connector("tupleConnectorOut");
        bloomTupleConnectorOut.setRelation(rel1);

        Connector bitmapConnectorOut = new Connector("bitmapConnectorOut");
        bitmapConnectorOut.setRelation(Relation.dummy);

        Bloom bloom = new Bloom(c1, bloomTupleConnectorOut, bitmapConnectorOut, jk1);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Relation rel2 = Relation.GetRelationByName(r2name);
        c2.setRelation(rel2);

        Connector bfilterTupleOut = new Connector("input2");
        bfilterTupleOut.setRelation(rel2);

        BFilter bfilter = new BFilter(c2, bitmapConnectorOut, bfilterTupleOut, jk2);

        Connector o = new Connector("output");
        Relation joinedRelation = Relation.join(rel1, rel2, jk1, jk2);
        o.setRelation(joinedRelation);

        HJoin hj = new HJoin(bloomTupleConnectorOut, bfilterTupleOut, o, jk1, jk2);

        Print p = new Print(o, joinedRelation.getRelationName());
        ThreadList.run(p);
    }
}
