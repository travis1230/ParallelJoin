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

    HSplit(Connector in, Connector connector_out[]) {
        this.in = in.getReadEnd();
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
                if (input == null) {
                    break;
                }
                int hash = myhash(input);
                out[hash].putNextString(input);
                //System.out.println( hash + " " + input );
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
