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
 * @author travisbrannen
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
}
