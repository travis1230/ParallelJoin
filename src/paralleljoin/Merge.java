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
public class Merge extends Thread implements GammaConstants {

    ReadEnd[] in;
    WriteEnd out;

    public Merge(Connector connector_in[], Connector connector_out) {
        this.out = connector_out.getWriteEnd();
        this.in = new ReadEnd[connector_in.length];
        for (int i = 0; i < in.length; i++) {
            this.in[i] = connector_in[i].getReadEnd();

        }
        ThreadList.add(this);
    }

    public void run() {
        try {
            Tuple nextTuple = null;

            for (int i = 0; i < in.length; i++) {
                while (true) {
                    nextTuple = in[i].getNextTuple();

                    if (nextTuple == null) {
                        break;
                    }

                    out.putNextTuple(nextTuple);
                }
            }
            out.close();
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
    }

    int myhash(String s) {
        return (Math.abs(s.hashCode()) % in.length);
    }
}
