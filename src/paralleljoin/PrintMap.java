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
public class PrintMap extends Thread implements GammaConstants{
    ReadEnd bitmapIn;
    
    public PrintMap(Connector connectorIn){
        bitmapIn = connectorIn.getReadEnd();
        ThreadList.add(this);
    }
    
    public void run(){
        try {
        /* read and print bit map */
            System.out.println(bitmapIn.getNextString());
        } catch (Exception e) { ReportError.msg(this, e); }
    }
}
