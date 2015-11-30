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
public class MSplit extends Thread implements GammaConstants{
    ReadEnd in;
    WriteEnd[] out;

    MSplit(Connector connector_in, Connector connector_out[]) {
        this.in = connector_in.getReadEnd();
        out = new WriteEnd[connector_out.length];
        for (int i=0; i<out.length; i++){
            this.out[i] = connector_out[i].getWriteEnd();
        
        }
        ThreadList.add(this);

    }

     public void run() {
        try {
            String input;
            while (true) {
                input = in.getNextString();
                //System.out.println( "input: " + input );

                if (input == null) {
                    break;
                }
                for(int i=0; i < out.length; ++i) {
                    String mask = BMap.mask(input, i);                   
                    //int hash = BMap.myhash(mask);
                    out[i].putNextString(mask);
                    //System.out.println( "output: " + hash + ": " + mask );
                }
            }
            for (int i = 0; i < out.length; i++) { out[i].close(); }
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }

    int myhash(String s) {
        return (Math.abs(s.hashCode()) % out.length);
    }

}
