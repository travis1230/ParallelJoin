/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test.paralleljoin;

import paralleljoin.*;
import org.junit.Test;

import gammaSupport.*;
import basicConnector.*;


/**
 *
 * @author travisbrannen (modified batory template code)
 */
public class MainTest {

    public MainTest() {
        Utility.init();
    }

    @Test
    public void testReadRelation() throws Exception {
        Utility.redirectStdOut("Output/testReadRelationOut.txt");
        read("client");
        read("orders");
        read("parts");
        read("viewing");
        read("odetails");
        read("orders+odetails");
        read("parts+odetails");
        read("client+viewing");
        Utility.validate("Output/testReadRelationOut.txt",
                "Correct/testRR.txt", false);
    }

    public void read(String filename) throws Exception {
        System.out.println("Reading " + filename);
        ThreadList.init();
        Connector readToPrint = new Connector("readToPrint");
        ReadRelation r = new ReadRelation(filename, readToPrint);
        readToPrint.setRelation(Relation.GetRelationByName(filename));
        Print p = new Print(readToPrint, filename);
        ThreadList.run(p);
    }

    @Test
    public void testHJoin() throws Exception {

        Utility.redirectStdOut("Output/testHJoinOut.txt");
        join("parts", "odetails", 0, 1);
        join("client", "viewing", 0, 0);
        join("orders", "odetails", 0, 0);
        Utility.validate("Output/testHJoinOut.txt", "Correct/testHJoin.txt", true);
    }

    public void join(String r1name, String r2name, int jk1, int jk2
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
        Connector o = new Connector("output");
        HJoin hj = new HJoin(c1, c2, o, jk1, jk2);
        String joinedName = r1name + "+" + r2name;
        Relation joinedRelation = Relation.join(rel1, rel2, jk1, jk2);
        o.setRelation(joinedRelation);
        Print p = new Print(o, joinedRelation.getRelationName());
        ThreadList.run(p);
    }

    @Test
    public void testParallelHJoin() throws Exception {
        Utility.redirectStdOut("Output/testParallelHJoinOut.txt");
        parallelJoin("parts", "odetails", 0, 1);
        parallelJoin("client", "viewing", 0, 0);
        parallelJoin("orders", "odetails", 0, 0);
        Utility.validate("Output/testParallelHJoinOut.txt", "Correct/testParallelHJoin.txt", true);
    }

    public void parallelJoin(String r1name, String r2name, int jk1, int jk2
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

    @Test
    public void testHSplit() throws Exception {
        Utility.redirectStdOut("Output/testHSplitOut.txt");
        splitToTwo("client", 0);
        splitToTwo("orders", 0);
        splitToTwo("parts", 0);
        splitToTwo("viewing", 0);
        splitToTwo("odetails", 0);
        splitToTwo("orders+odetails", 0);
        splitToTwo("parts+odetails", 0);
        splitToTwo("client+viewing", 0);
        Utility.validate("Output/testHSplitOut.txt", "Correct/testHSplit.txt", true);
    }

    public void splitToTwo(String rname, int jk) throws Exception {
        // read-->hash--0-->print1
        //            --1-->print2
        System.out.println("Splitting " + rname);
        for (int i = 0; i < 2; i++) {  // Do it twice to print appropriately
            ThreadList.init();
            Connector read_hash = new Connector("readToHash");
            ReadRelation r = new ReadRelation(rname, read_hash);
            Relation rel = Relation.GetRelationByName(rname);
            read_hash.setRelation(rel);
            Connector[] hash_out_connect;
            hash_out_connect = new Connector[2];
            hash_out_connect[0] = new Connector("hashOutput1");
            hash_out_connect[1] = new Connector("hashOutput2");
            hash_out_connect[0].setRelation(rel);
            hash_out_connect[1].setRelation(rel);
            HSplit h = new HSplit(read_hash, hash_out_connect, jk);
            System.out.println("Output set " + i + ":");
            Print p = new Print(hash_out_connect[i], rname);
            ThreadList.run(p);
        }
    }

    @Test
    public void testBFilter() throws Exception {
        Utility.redirectStdOut("Output/testBFilterOut.txt");
        simulateBloomFilter("parts", 0);
        simulateBloomFilter("parts", 1);
        simulateBloomFilter("parts", 2);
        simulateBloomFilter("client", 2);
        Utility.validate("Output/testBFilterOut.txt", "Correct/testBFilter.txt", true);
    }

    public void simulateBloomFilter(String rname, int joinKey) throws Exception {
        System.out.println("Bloom Filtering " + rname
                + " on joinKey " + joinKey);
        ThreadList.init();
        Connector read_filter = new Connector("readToFilter");
        ReadRelation r = new ReadRelation(rname, read_filter);
        Relation rel = Relation.GetRelationByName(rname);  // created by r
        read_filter.setRelation(rel);  // set now after r creates
        Connector simulator_filter = new Connector("simToFilter");
        BloomSimulator bs = new BloomSimulator(simulator_filter);
        simulator_filter.setRelation(Relation.dummy);
        Connector filter_print = new Connector("filterToPrint");
        filter_print.setRelation(rel);  // same relation created by r
        BFilter bf = new BFilter(read_filter, simulator_filter,
                filter_print, joinKey);
        Print p = new Print(filter_print, rname);
        ThreadList.run(p);
    }

    @Test
    public void testParallelBFilter() throws Exception {
        Utility.redirectStdOut("Output/testParallelBFilterOut.txt");
        simulateParallelBloomFilter("parts", 0);
        simulateParallelBloomFilter("parts", 1);
        simulateParallelBloomFilter("parts", 2);
        simulateParallelBloomFilter("client", 2);
        Utility.validate("Output/testParallelBFilterOut.txt", "Correct/testBFilter.txt", true);
    }

    public void simulateParallelBloomFilter(String rname, int joinKey) throws Exception {
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

    @Test
    public void testBloom() throws Exception {
        Utility.redirectStdOut("Output/testBloomOut.txt");
        bloom("client", 0);
        bloom("orders", 1);
        bloom("parts", 0);
        bloom("viewing", 1);
        bloom("odetails", 0);
        bloom("orders+odetails", 1);
        bloom("parts+odetails", 0);
        bloom("client+viewing", 1);
        Utility.validate("Output/testBloomOut.txt", "Correct/testBloom.txt", true);
    }

    public void bloom(String rname, int joinKey) throws Exception {
        for (int i = 0; i < 2; i++) {
            System.out.println("Blooming " + rname);
            ThreadList.init();
            Connector readBloom = new Connector("readToBloom");
            ReadRelation r = new ReadRelation(rname, readBloom);
            Relation rel = Relation.GetRelationByName(rname);
            readBloom.setRelation(rel);
            Connector bloomPrintMap = new Connector("BloomToPrintBitmap");
            bloomPrintMap.setRelation(Relation.dummy);
            Connector bloomPrintTuples = new Connector("BloomToPrintTuples");
            bloomPrintTuples.setRelation(rel);
            Bloom b = new Bloom(readBloom, bloomPrintTuples, bloomPrintMap,
                    joinKey);
            if (i == 0) {
                PrintMap pMap = new PrintMap(bloomPrintMap);
                ThreadList.run(pMap);
            } else {
                Print pTuples = new Print(bloomPrintTuples, rname);
                ThreadList.run(pTuples);
            }
        }
    }

    @Test
    public void testParallelBloom() throws Exception {
        Utility.redirectStdOut("Output/testParallelBloomOut.txt");
        parallelBloom("client", 0);
        parallelBloom("orders", 1);
        parallelBloom("parts", 0);
        parallelBloom("viewing", 1);
        parallelBloom("odetails", 0);
        parallelBloom("orders+odetails", 1);
        parallelBloom("parts+odetails", 0);
        parallelBloom("client+viewing", 1);
        Utility.validate("Output/testParallelBloomOut.txt", "Correct/testBloom.txt", true);
    }

    public void parallelBloom(String rname, int joinKey) throws Exception {
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

    @Test
    public void testMerge() throws Exception {
        Utility.redirectStdOut("Output/testMergeOut.txt");
        merge("parts", "parts");
        merge("client", "client");
        merge("odetails", "odetails");
        Utility.validate("Output/testMergeOut.txt", "Correct/testMerge.txt", true);
    }

    public void merge(String r1name, String r2name) throws Exception {
        System.out.println("Merging " + r1name + " with " + r2name);
        ThreadList.init();

        Connector c1 = new Connector("input1");
        ReadRelation r1 = new ReadRelation(r1name, c1);
        Relation rel1 = Relation.GetRelationByName(r1name);
        c1.setRelation(rel1);
        Connector c2 = new Connector("input2");
        ReadRelation r2 = new ReadRelation(r2name, c2);
        Relation rel2 = Relation.GetRelationByName(r2name);
        c2.setRelation(rel2);
        Connector[] i = {c1, c2};
        Connector o = new Connector("output");
        Merge me = new Merge(i, o);

        o.setRelation(rel1);
        Print p = new Print(o, rel1.getRelationName());
        ThreadList.run(p);
    }

    @Test
    public void testMSplit() throws Exception {
        Utility.redirectStdOut("Output/testMSplitOut.txt");
        MSplitToFour("client", 0);
        MSplitToFour("orders", 1);
        MSplitToFour("parts", 0);
        MSplitToFour("viewing", 1);
        MSplitToFour("odetails", 0);
        MSplitToFour("orders+odetails", 1);
        MSplitToFour("parts+odetails", 0);
        MSplitToFour("client+viewing", 1);
        Utility.validate("Output/testMSplitOut.txt", "Correct/testMSplit.txt", true);
    }

    public void MSplitToFour(String rname, int joinKey) throws Exception {
        System.out.println("Splitting map for: " + rname);
        ThreadList.init();
        Connector readBloom = new Connector("readToBloom");
        ReadRelation r = new ReadRelation(rname, readBloom);
        Relation rel = Relation.GetRelationByName(rname);
        readBloom.setRelation(rel);
        Connector bloomPrintMap = new Connector("BloomToPrintBitmap");
        bloomPrintMap.setRelation(Relation.dummy);
        Connector bloomPrintTuples = new Connector("BloomToPrintTuples");
        bloomPrintTuples.setRelation(rel);
        Bloom b = new Bloom(readBloom, bloomPrintTuples, bloomPrintMap,
                joinKey);

        Connector c1 = new Connector("out1");
        c1.setRelation(Relation.dummy);

        Connector c2 = new Connector("out2");
        c2.setRelation(Relation.dummy);

        Connector c3 = new Connector("out3");
        c3.setRelation(Relation.dummy);

        Connector c4 = new Connector("out4");
        c4.setRelation(Relation.dummy);

        Connector[] o = {c1, c2, c3, c4};
        MSplit msplit = new MSplit(bloomPrintMap, o);

        PrintMap pMap = new PrintMap(c1);
        PrintMap pMap2 = new PrintMap(c2);
        PrintMap pMap3 = new PrintMap(c3);
        PrintMap pMap4 = new PrintMap(c4);
        ThreadList.run(pMap4);
    }

    @Test
    public void testMMerge() throws Exception {
        Utility.redirectStdOut("Output/testMMergeOut.txt");
        MMergeFour("client", 0);
        MMergeFour("orders", 1);
        MMergeFour("parts", 0);
        MMergeFour("viewing", 1);
        MMergeFour("odetails", 0);
        MMergeFour("orders+odetails", 1);
        MMergeFour("parts+odetails", 0);
        MMergeFour("client+viewing", 1);
        Utility.validate("Output/testMMergeOut.txt", "Correct/testMMerge.txt", true);
    }

    public void MMergeFour(String rname, int joinKey) throws Exception {
        System.out.println("Splitting map for: " + rname);
        ThreadList.init();
        Connector readBloom = new Connector("readToBloom");
        ReadRelation r = new ReadRelation(rname, readBloom);
        Relation rel = Relation.GetRelationByName(rname);
        readBloom.setRelation(rel);
        Connector bloomPrintMap = new Connector("BloomToPrintBitmap");
        bloomPrintMap.setRelation(Relation.dummy);
        Connector bloomPrintTuples = new Connector("BloomToPrintTuples");
        bloomPrintTuples.setRelation(rel);
        Bloom b = new Bloom(readBloom, bloomPrintTuples, bloomPrintMap,
                joinKey);

        Connector c1 = new Connector("out1");
        c1.setRelation(Relation.dummy);

        Connector c2 = new Connector("out2");
        c2.setRelation(Relation.dummy);

        Connector c3 = new Connector("out3");
        c3.setRelation(Relation.dummy);

        Connector c4 = new Connector("out4");
        c4.setRelation(Relation.dummy);

        Connector[] o = {c1, c2, c3, c4};
        MSplit msplit = new MSplit(bloomPrintMap, o);

        Connector combined = new Connector("combined");
        combined.setRelation(Relation.dummy);

        MMerge mmerge = new MMerge(o, combined);
        PrintMap pMap = new PrintMap(combined);

        ThreadList.run(pMap);
    }

    @Test
    public void testHJoinFirstRefinement() throws Exception {
        Utility.redirectStdOut("Output/testHJoinFirstRefinementOut.txt");
        JoinFirstRefinement("parts", "odetails", 0, 1);
        JoinFirstRefinement("client", "viewing", 0, 0);
        JoinFirstRefinement("orders", "odetails", 0, 0);
        Utility.validate("Output/testHJoinFirstRefinementOut.txt", "Correct/testHJoin.txt", true);
    }

    public void JoinFirstRefinement(String r1name, String r2name, int jk1, int jk2
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

    @Test
    public void testOptimizedParallelHJoin() throws Exception {
        Utility.redirectStdOut("Output/testOptimizedParallelHJoinOut.txt");
        OptimizedParallelHJoin("parts", "odetails", 0, 1);
        OptimizedParallelHJoin("client", "viewing", 0, 0);
        OptimizedParallelHJoin("orders", "odetails", 0, 0);
        Utility.validate("Output/testOptimizedParallelHJoinOut.txt", "Correct/testHJoin.txt", true);
    }

    public void OptimizedParallelHJoin(String r1name, String r2name, int jk1, int jk2
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

    @Test
    public void testUnOptimizedParallelHJoin() throws Exception {
        Utility.redirectStdOut("Output/testUnOptimizedParallelHJoinOut.txt");
        UnOptimizedParallelHJoin("parts", "odetails", 0, 1);
        UnOptimizedParallelHJoin("client", "viewing", 0, 0);
        UnOptimizedParallelHJoin("orders", "odetails", 0, 0);
        Utility.validate("Output/testUnOptimizedParallelHJoinOut.txt", "Correct/testHJoin.txt", true);
    }

    public void UnOptimizedParallelHJoin(String r1name, String r2name, int jk1, int jk2
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

        Connector bloomTuple = new Connector("BloomToPrintTuples");
        bloomTuple.setRelation(rel1);

        Connector bloomBitMap = new Connector("bloomPrintMap");
        bloomBitMap.setRelation(Relation.dummy);

        Merge merge = new Merge(bloomTupleConnectorOuts, bloomTuple);
        MMerge mmerge = new MMerge(bitmapConnectorOuts, bloomBitMap);

        Connector bloomTupleSplit[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < bloomTupleSplit.length; ++i) {
            bloomTupleSplit[i] = new Connector("bloomTupleSplit" + i);
            bloomTupleSplit[i].setRelation(rel1);
        }

        HSplit hs = new HSplit(bloomTuple, bloomTupleSplit, jk1);

        Connector bloomBitMapSplit[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < bloomBitMapSplit.length; ++i) {
            bloomBitMapSplit[i] = new Connector("bloomBitMapSplit" + i);
            bloomBitMapSplit[i].setRelation(Relation.dummy);
        }

        MSplit ms = new MSplit(bloomBitMap, bloomBitMapSplit);

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

        Connector[] bfilterTupleOuts = new Connector[GammaConstants.splitLen];
        BFilter[] bfilters = new BFilter[GammaConstants.splitLen];
        for (int i = 0; i < GammaConstants.splitLen; ++i) {
            bfilterTupleOuts[i] = new Connector("bfilterTupleOuts" + i);
            bfilterTupleOuts[i].setRelation(rel2);

            bfilters[i] = new BFilter(c2Split[i], bloomBitMapSplit[i], bfilterTupleOuts[i], jk2);
        }

        Connector bfilterTuple = new Connector("bfilterTuple");
        bfilterTuple.setRelation(rel2);

        Merge merge2 = new Merge(bfilterTupleOuts, bfilterTuple);

        Connector bfilterTupleSplit[] = new Connector[GammaConstants.splitLen];
        for (int i = 0; i < bfilterTupleSplit.length; ++i) {
            bfilterTupleSplit[i] = new Connector("bfilterTupleSplit" + i);
            bfilterTupleSplit[i].setRelation(rel2);
        }

        HSplit h3 = new HSplit(bfilterTuple, bfilterTupleSplit, jk2);

        Connector[] outs = new Connector[GammaConstants.splitLen];
        Relation joinedRelation = Relation.join(rel1, rel2, jk1, jk2);

        for (int i = 0; i < GammaConstants.splitLen; ++i) {
            outs[i] = new Connector("output" + i);
            outs[i].setRelation(joinedRelation);
            HJoin hj = new HJoin(bloomTupleSplit[i], bfilterTupleSplit[i], outs[i], jk1, jk2);
        }

        Connector out = new Connector("final output");
        out.setRelation(joinedRelation);

        Merge merge3 = new Merge(outs, out);

        Print p = new Print(out, joinedRelation.getRelationName());
        ThreadList.run(p);
    }

}
