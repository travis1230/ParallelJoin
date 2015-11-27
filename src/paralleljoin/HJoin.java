/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Iterator;

import basicConnector.*;
import gammaSupport.*;

/**
 *
 * @author travisbrannen
 */
public class HJoin extends Thread implements GammaConstants{
    
    ReadEnd[] in;
    WriteEnd w;
    int[] jk;
    
    
    public HJoin(Connector in0, Connector in1, Connector out, int jk0, int jk1) {
        in = new ReadEnd[2];
        in[0] = in0.getReadEnd();
        in[1] = in1.getReadEnd();
        
        w = out.getWriteEnd();
        
        jk = new int[2];
        jk[0] = jk0;
        jk[1] = jk1;
        
        ThreadList.add(this);
    }
    public void run(){

        // 1. read all of stream A into a main memory hash table
        HashMap<String, LinkedList<Tuple>> joinHash = new HashMap<
                String, LinkedList<Tuple>>();
        Tuple nextTuple;
        try {
            while (true){
                nextTuple = in[0].getNextTuple();
                if (nextTuple == null){
                    break;
                }
                // joinHash = first table
                LinkedList<Tuple> ll = new LinkedList();
                ll.add(nextTuple);
                joinHash.put(nextTuple.get(jk[0]), ll);
            }
        // 2. read B stream one tuple at a time;
        // hash join key of B’s tuple and join it to all A tuple’s with the same join key
        // linear algorithm in that each A,B tuples is read only once
            while (true){
                nextTuple = in[1].getNextTuple();
                if (nextTuple == null){
                    break;
                }
                // add second table one tuple at a time, joining with shared
                // joinkey from first table
                String matchingKey = nextTuple.get(jk[1]);
                LinkedList<Tuple> ll = joinHash.get(matchingKey);
                Tuple t = ll.getFirst();  // first one is the unjoined
                Tuple joinedTuple = Tuple.join(t, nextTuple, jk[0], jk[1]);
                ll.add(joinedTuple);
                joinHash.put(matchingKey, ll);
            }
        // 3.  Pass joinHash tuples to output pipe
            for (Map.Entry<String, LinkedList<Tuple>> entry : joinHash.entrySet()){
                Iterator<Tuple> iterator = entry.getValue().iterator();
                boolean first = true;
                while(iterator.hasNext()){
                    Tuple t = iterator.next();
                    if (first){ first = false; }  // first is unjoined
                    else { w.putNextTuple(t); }
                }
            }
            w.close();
        } catch (Exception e) { ReportError.msg(this, e); }    
    }
}
