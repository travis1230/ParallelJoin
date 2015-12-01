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
import paralleljoin.Bloom;
import paralleljoin.HSplit;
import paralleljoin.MMerge;
import paralleljoin.Merge;
import paralleljoin.Print;
import paralleljoin.PrintMap;
import paralleljoin.ReadRelation;

/**
 *
 * @author floydgb
 */
public class MapReduceBloom {
    public void runMapReduceBloom(String rname, int joinKey) throws Exception {
        for (int i = 0; i < 2; i++) {
            System.out.println("Blooming " + rname);
            ThreadList.init();

            Connector readBloom = new Connector("readToBloom");
            ReadRelation r = new ReadRelation(rname, readBloom);
            Relation rel = Relation.GetRelationByName(rname);
            readBloom.setRelation(rel);

            Connector c1Split[] = new Connector[GammaConstants.splitLen];
            for (int j = 0; j < c1Split.length; ++j) {
                c1Split[j] = new Connector("c1Split" + j);
                c1Split[j].setRelation(rel);
            }

            HSplit h = new HSplit(readBloom, c1Split, joinKey);

            Connector[] bloomPrintMap = new Connector[GammaConstants.splitLen];
            Connector[] bloomPrintTuples = new Connector[GammaConstants.splitLen];
            for (int j = 0; j < c1Split.length; ++j) {
                bloomPrintMap[j] = new Connector("BloomToPrintBitmap");
                bloomPrintMap[j].setRelation(Relation.dummy);

                bloomPrintTuples[j] = new Connector("BloomToPrintTuples");
                bloomPrintTuples[j].setRelation(rel);

                Bloom b = new Bloom(c1Split[j], bloomPrintTuples[j], bloomPrintMap[j],
                        joinKey);
            }
            Connector bloomPrintTuple = new Connector("BloomToPrintTuples");
            bloomPrintTuple.setRelation(rel);

            Connector bloomPrintM = new Connector("bloomPrintMap");
            bloomPrintM.setRelation(Relation.dummy);

            Merge merge = new Merge(bloomPrintTuples, bloomPrintTuple);
            MMerge mmerge = new MMerge(bloomPrintMap, bloomPrintM);

            if (i == 0) {
                PrintMap pMap = new PrintMap(bloomPrintM);
                ThreadList.run(pMap);
            } else {
                Print pTuples = new Print(bloomPrintTuple, rname);
                ThreadList.run(pTuples);
            }
        }
    }
}
