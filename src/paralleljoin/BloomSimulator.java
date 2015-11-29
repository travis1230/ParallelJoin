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
public class BloomSimulator extends Thread implements GammaConstants{
    WriteEnd w;
    public BloomSimulator (Connector bitmapOut){
        w = bitmapOut.getWriteEnd();
        ThreadList.add(this);
    }
    public void run(){
        try {
            BMap m = BMap.makeBMap();
            for (Integer i=0; i<10; i++){
                m.setValue(i.toString(), true);
            }
            w.putNextString(m.getBloomFilter());
            w.close();
        } catch (Exception e) { ReportError.msg(this, e); }
    }
}
