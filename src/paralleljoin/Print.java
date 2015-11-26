/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import java.io.BufferedReader;
import java.io.IOException;

import gammaSupport.*;
import basicConnector.*;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author travisbrannen
 */
public class Print extends Thread{
    ReadEnd in;
    Relation r;  // required for formatting
    
    public Print( Connector in, Relation relation) {
        this.in = in.getReadEnd();
        r = relation;
        ThreadList.add(this);
    }

    public void run() {
        String input;

        try {
            while (true) {
                input = in.getNextString();
                if (input == null) {
                    break;
                }
                printTuple(input);
            }
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
        System.out.flush();

    }
    
    public void printTuple(String tupleString){
        Tuple t = Tuple.makeTupleFromPipeData(tupleString);
        t.println(r);
    }
}
