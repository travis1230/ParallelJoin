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
 * @author Don
 */
public class HSplit extends Thread implements GammaConstants{
    ReadEnd in;
    WriteEnd[] out;
    int jk;

    public HSplit(Connector in, Connector connector_out[], int joinKey) {
        this.in = in.getReadEnd();
        out = new WriteEnd[connector_out.length];
        for (int i=0; i<out.length; i++){
            this.out[i] = connector_out[i].getWriteEnd();
        
        }
        jk = joinKey;
        ThreadList.add(this);

    }

     public void run() {
        try {
            Tuple input;           
            while (true) {
                input = in.getNextTuple();
                if (input == null) {
                    break;
                }
                int hash = BMap.myhash(input.get(jk)) % out.length;
                //System.out.println( hash + " " + input );
                out[hash].putNextTuple(input);
            }
            for (int i = 0; i < out.length; i++) { out[i].close(); }
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }
}
