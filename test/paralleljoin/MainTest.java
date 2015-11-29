/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import org.junit.Test;

import gammaSupport.*;
import basicConnector.*;

import RegTest.Utility;

/**
 *
 * @author travisbrannen (modified batory template code)
 */
public class MainTest {
    
    public MainTest() {
        RegTest.Utility.init();
    }

    @Test
    public void testReadRelation() throws Exception {
        RegTest.Utility.redirectStdOut("testReadRelationOut.txt");
        read("client");
        read("orders");
        read("parts");
        read("viewing");
        read("odetails");
        read("orders+odetails");
        read("parts+odetails");
        read("client+viewing");
        RegTest.Utility.validate("testReadRelationOut.txt", 
                "Correct/testRR.txt", false);
    }
    
    public void read(String filename) throws Exception{
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
        // TODO: fix testHJoin to be correct when all joinKeys are unique
        // fix HJoin to not store list of all matches for a joinKey (since there
        // should only be 1)
        Utility.redirectStdOut("testHJoinOut.txt");
        join("parts", "odetails", 0, 1);
        join("client", "viewing", 0, 0);
        join("orders", "odetails", 0, 0);
        Utility.validate("testHJoinOut.txt", "Correct/testHJoin.txt",true);
    }

    public void join(String r1name, String r2name, int jk1, int jk2
    ) throws Exception {
        System.out.println( "Joining " + r1name + " with " + r2name );
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
    public void testHSplit() throws Exception {
        RegTest.Utility.redirectStdOut("testHSplitOut.txt");
        splitToTwo("client");
        splitToTwo("orders");
        splitToTwo("parts");
        splitToTwo("viewing");
        splitToTwo("odetails");
        splitToTwo("orders+odetails");
        splitToTwo("parts+odetails");
        splitToTwo("client+viewing");
        Utility.validate("testHSplitOut.txt", "Correct/testHSplit.txt",true);
    }
    
    public void splitToTwo(String rname) throws Exception {
        // read-->hash--0-->print1
        //            --1-->print2
        System.out.println("Splitting " + rname);
        for (int i=0; i<2; i++){  // Do it twice to print appropriately
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
            HSplit h = new HSplit(read_hash, hash_out_connect);
            System.out.println("Output set " + i + ":");
            Print p = new Print(hash_out_connect[i], rname);
            ThreadList.run(p);
        }
    }
    
    @Test
    public void testBFilter() throws Exception{
        Utility.redirectStdOut("testBFilterOut.txt");
        simulateBloomFilter("parts", 0);
        simulateBloomFilter("parts", 1);
        simulateBloomFilter("parts", 2);
        simulateBloomFilter("client", 2);
        Utility.validate("testBFilterOut.txt", "Correct/testBFilter.txt",true);
    }
    
    public void simulateBloomFilter(String rname, int joinKey) throws Exception{
        System.out.println("Bloom Filtering " + rname + 
                " on joinKey " + joinKey);
        ThreadList.init();
        Connector read_filter = new Connector("readToFilter");
        ReadRelation r = new ReadRelation(rname,read_filter);
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
    public void testBloom() throws Exception{
        Utility.redirectStdOut("testBloomOut.txt");
        bloom("client", 0);
        bloom("orders", 1);
        bloom("parts", 0);
        bloom("viewing", 1);
        bloom("odetails", 0);
        bloom("orders+odetails", 1);
        bloom("parts+odetails", 0);
        bloom("client+viewing", 1);
        Utility.validate("testBloomOut.txt", "Correct/testBloom.txt",true);
    }
    
    public void bloom(String rname, int joinKey) throws Exception {
        for (int i=0; i<2; i++){
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
            if (i==0){ 
                PrintMap pMap = new PrintMap(bloomPrintMap);
                ThreadList.run(pMap);
            } else { 
                Print pTuples = new Print(bloomPrintTuples, rname);
                ThreadList.run(pTuples);
            }
        }
    }
}
