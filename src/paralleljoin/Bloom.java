/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import basicConnector.*;
import gammaSupport.*;

/**
 *
 * @author travisbrannen
 */
public class Bloom extends Thread implements GammaConstants{
    ReadEnd tupleIn;
    WriteEnd tupleOut;
    WriteEnd bitmapOut;
    int jk;
    
    public Bloom(Connector tupleConnectorIn, Connector tupleConnectorOut,
            Connector bitmapConnectorOut, int joinKey){
        tupleIn = tupleConnectorIn.getReadEnd();
        tupleOut = tupleConnectorOut.getWriteEnd();
        bitmapOut = bitmapConnectorOut.getWriteEnd();
        jk = joinKey;
        ThreadList.add(this);
    }
    
    public void run(){
        // Bloom filtering is a common technique for disqualifying tuples from further processing
        try {
        // 1. clear bit map M
            BMap m = BMap.makeBMap();
        // 2. read each A tuple, hash its join key, and mark corresponding bit in M
            Tuple t;
            while (true) {
                t = tupleIn.getNextTuple();
                if (t == null){
                    tupleOut.close();
                    break;
                }
                m.setValue(t.get(jk), true);
                //System.out.println("Tuple:" + t.toString() + "\nHash Filter: " + m.getBloomFilter());
        // 3. output each tuple A
                tupleOut.putNextTuple(t);
            }
        // 4. after all A tuples read, output M
            bitmapOut.putNextString(m.getBloomFilter());
            bitmapOut.close();
        } catch (Exception e) { ReportError.msg(this, e); }
    }
}
