/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import org.junit.Test;

import gammaSupport.*;
import basicConnector.*;
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
        RegTest.Utility.redirectStdOut("out.txt");
        
        ThreadList.init();
        Connector readToPrint = new Connector("readToPrint");
        ReadRelation r = new ReadRelation("client", readToPrint);
        readToPrint.setRelation(Relation.GetRelationByName("client"));
        Print p = new Print(readToPrint, Relation.GetRelationByName("client"));
        
        ThreadList.run(p);
        
        RegTest.Utility.validate("out.txt", "Correct/testRR.txt", false);
    }
    
}
