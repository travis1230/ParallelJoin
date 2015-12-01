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
import paralleljoin.BloomSimulator;
import paralleljoin.HSplit;
import paralleljoin.MSplit;
import paralleljoin.Merge;
import paralleljoin.Print;
import paralleljoin.ReadRelation;

/**
 *
 * @author floydgb
 */
public class MapReduceBFilter {
    public void runMapReduceBFilter(String rname, int joinKey) throws Exception {
        System.out.println("Bloom Filtering " + rname
                + " on joinKey " + joinKey);
        ThreadList.init();

        Connector read_filter = new Connector("readToFilter");
        ReadRelation r = new ReadRelation(rname, read_filter);
        Relation rel = Relation.GetRelationByName(rname);  // created by r
        read_filter.setRelation(rel);  // set now after r creates

        Connector c1Split[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < c1Split.length; ++i) {
            c1Split[i] = new Connector("c1Split" + i);
            c1Split[i].setRelation(rel);
        }

        HSplit h = new HSplit(read_filter, c1Split, joinKey);

        Connector simulator_filter = new Connector("simToFilter");
        BloomSimulator bs = new BloomSimulator(simulator_filter);
        simulator_filter.setRelation(Relation.dummy);

        Connector[] filterSplit = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < filterSplit.length; ++i) {
            filterSplit[i] = new Connector("c1Split" + i);
            filterSplit[i].setRelation(rel);
        }

        MSplit ms = new MSplit(simulator_filter, filterSplit);

        Connector[] filter_print = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < GammaConstants.splitLen; ++i) {
            filter_print[i] = new Connector("filterToPrint");
            filter_print[i].setRelation(rel);  // same relation created by r
            BFilter bf = new BFilter(c1Split[i], filterSplit[i],
                    filter_print[i], joinKey);
        }

        Connector f_print = new Connector("filterToPrint");
        f_print.setRelation(rel);
        Merge m = new Merge(filter_print, f_print);

        Print p = new Print(f_print, rname);
        ThreadList.run(p);
    }
}
