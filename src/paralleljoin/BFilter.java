/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import gammaSupport.*;
import basicConnector.*;

/**
 *
 * @author travisbrannen
 */
public class BFilter extends Thread implements GammaConstants{
    ReadEnd tupleIn;
    ReadEnd bitmapIn;
    WriteEnd tupleOut;
    int jk;  //join key to bloom filter on
    
    public BFilter(Connector connector_tuple_in, 
            Connector connector_bitmap_in,
            Connector connector_tuple_out, int joinKey){
        tupleIn = connector_tuple_in.getReadEnd();
        bitmapIn = connector_bitmap_in.getReadEnd();
        tupleOut = connector_tuple_out.getWriteEnd();
        jk = joinKey;
        ThreadList.add(this);
    }
    
    public void run(){
        /*The filtering part of Bloom filters
        • eliminates B tuples that cannot join with A tuples
        • Algorithm:*/
        try {
        /*1. read bit map M*/
            String nextString = bitmapIn.getNextString();
            BMap m = BMap.makeBMap(nextString);
            //System.out.println(m.getBloomFilter());
            while (true){
        /*2. read each tuple of B, hash its join key: if corresponding bit in M is set
        output tuple*/
                Tuple nextTuple = tupleIn.getNextTuple();
                if (nextTuple == null){
                    tupleOut.close();
                    break;
                }
                if (m.getValue(nextTuple.get(jk))){
                    //System.out.println("Forwarding " + nextTuple.toString());
                    tupleOut.putNextTuple(nextTuple);
                } //else {
                    //System.out.println("Filtering " + nextTuple.toString());
                //}
        /*3. else discard tuple as it will never join with A tuples*/
            }
        } catch (Exception e) { ReportError.msg(this, e); }
    }
}
