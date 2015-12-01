/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package paralleljoin;

import java.io.*;
import gammaSupport.*;
import basicConnector.*;

/**
 *
 * @author floydgb
 */
public class MMerge extends Thread implements GammaConstants{
    ReadEnd[] in;
    WriteEnd out;

    public MMerge(Connector connector_in[], Connector connector_out) {
        this.out = connector_out.getWriteEnd();
        this.in = new ReadEnd[connector_in.length];
        for (int i=0; i<in.length; i++){
            this.in[i] = connector_in[i].getReadEnd();
            
        }
        ThreadList.add(this);
    }

     public void run() {
        try {
            BMap[] nextBMaps = new BMap[in.length];
            
            for (int i=0; i<in.length; i++){
                nextBMaps[i] = BMap.makeBMap(in[i].getNextString());
            }

            BMap nextBMap = nextBMaps[0];
            for (int i=1; i<in.length; i++){
                if(nextBMaps[i] != null){
                    nextBMap = BMap.or(nextBMap, nextBMaps[i]);
                } 
            }

            out.putNextString(nextBMap.getBloomFilter());

            out.close();
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
