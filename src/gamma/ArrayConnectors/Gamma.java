/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gamma.ArrayConnectors;

import basicConnector.Connector;
import gammaSupport.GammaConstants;
import gammaSupport.Relation;
import gammaSupport.ThreadList;
import paralleljoin.BFilter;
import paralleljoin.Bloom;
import paralleljoin.HJoin;
import paralleljoin.HSplit;
import paralleljoin.Merge;
import paralleljoin.Print;
import paralleljoin.ReadRelation;

/**
 *
 * @author floydgb
 */
public class Gamma {
    public void runGamma(String r1name, String r2name, int jk1, int jk2
    ) throws Exception {
        System.out.println("Joining " + r1name + " with " + r2name);
        ThreadList.init();

        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1);
        Relation rel1 = Relation.GetRelationByName(r1name);
        c1.setRelation(rel1);

        Connector c1Split[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < c1Split.length; ++i) {
            c1Split[i] = new Connector("c1Split" + i);
            c1Split[i].setRelation(rel1);
        }

        HSplit h = new HSplit(c1, c1Split, jk1);

        Connector[] bloomTupleConnectorOuts = new Connector[GammaConstants.splitLen];
        Connector[] bitmapConnectorOuts = new Connector[GammaConstants.splitLen];
        Bloom[] bloom = new Bloom[GammaConstants.splitLen];

        for (int i = 0; i < c1Split.length; ++i) {
            bloomTupleConnectorOuts[i] = new Connector("tupleConnectorOut" + i);
            bloomTupleConnectorOuts[i].setRelation(rel1);

            bitmapConnectorOuts[i] = new Connector("bitmapConnectorOut" + i);
            bitmapConnectorOuts[i].setRelation(Relation.dummy);

            bloom[i] = new Bloom(c1Split[i], bloomTupleConnectorOuts[i], bitmapConnectorOuts[i], jk1);
        }

        Connector bitmapConnectorOut = new Connector("bitmapConnectorOut");
        bitmapConnectorOut.setRelation(Relation.dummy);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Relation rel2 = Relation.GetRelationByName(r2name);
        c2.setRelation(rel2);

        Connector c2Split[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < c2Split.length; ++i) {
            c2Split[i] = new Connector("c2Split" + i);
            c2Split[i].setRelation(rel2);
        }

        HSplit h2 = new HSplit(c2, c2Split, jk2);

        Connector[] outs = new Connector[GammaConstants.splitLen];
        Relation joinedRelation = Relation.join(rel1, rel2, jk1, jk2);

        Connector[] bfilterTupleOuts = new Connector[GammaConstants.splitLen];
        BFilter[] bfilters = new BFilter[GammaConstants.splitLen];
        for (int i = 0; i < c2Split.length; ++i) {
            bfilterTupleOuts[i] = new Connector("bfilterTupleOuts" + i);
            bfilterTupleOuts[i].setRelation(rel2);

            outs[i] = new Connector("output" + i);
            outs[i].setRelation(joinedRelation);

            bfilters[i] = new BFilter(c2Split[i], bitmapConnectorOuts[i], bfilterTupleOuts[i], jk2);

            HJoin hj = new HJoin(bloomTupleConnectorOuts[i], bfilterTupleOuts[i], outs[i], jk1, jk2);
        }

        Connector out = new Connector("final output");
        out.setRelation(joinedRelation);

        Merge merge = new Merge(outs, out);

        Print p = new Print(out, joinedRelation.getRelationName());
        ThreadList.run(p);
    }
}
