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
import paralleljoin.HJoin;
import paralleljoin.HSplit;
import paralleljoin.Merge;
import paralleljoin.Print;
import paralleljoin.ReadRelation;

/**
 *
 * @author floydgb
 */
public class MapReduceHJoin {
    public void runMapReduceHjoin(String r1name, String r2name, int jk1, int jk2
    ) throws Exception {
        System.out.println("Joining " + r1name + " with " + r2name);
        ThreadList.init();

        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1);
        Relation rel1 = Relation.GetRelationByName(r1name);
        c1.setRelation(rel1);

        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Relation rel2 = Relation.GetRelationByName(r2name);
        c2.setRelation(rel2);

        String joinedName = r1name + "+" + r2name;
        Relation joinedRelation = Relation.join(rel1, rel2, jk1, jk2);

        Connector c1Split[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < c1Split.length; ++i) {
            c1Split[i] = new Connector("c1Split" + i);
            c1Split[i].setRelation(joinedRelation);
        }

        HSplit h = new HSplit(c1, c1Split, jk1);

        Connector c2Split[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < c2Split.length; ++i) {
            c2Split[i] = new Connector("c2Split" + i);
            c2Split[i].setRelation(rel2);
        }

        HSplit h2 = new HSplit(c2, c2Split, jk2);

        Connector[] outs = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < GammaConstants.splitLen; ++i) {
            outs[i] = new Connector("output");
            outs[i].setRelation(joinedRelation);

            HJoin hj = new HJoin(c2Split[i], c2Split[i], outs[i], jk1, jk2);

        }

        Connector o = new Connector("output");
        o.setRelation(joinedRelation);

        Merge m = new Merge(outs, o);

        Print p = new Print(o, joinedRelation.getRelationName());
        ThreadList.run(p);
    }
}
