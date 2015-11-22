/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package basicConnector;

import gammaSupport.Relation;
import gammaSupport.Tuple;
import java.io.BufferedReader;

/**
 *
 * @author dsb
 */
// this class is an adaptor that maps a Java pipe interface to a TupleStream
// interface
// YOU READ FROM THE CONNECTOR ON THIS END (ARROW END)
// this.in REFERS TO INPUT FOR CONNECTOR'S RECEIVER
public class ReadEnd  {
    BufferedReader b;  // connector's BufferedReader, where it puts new inputs
    Connector c;  // connector to which this is ReadEnd of

    // constructors
    ReadEnd( Connector c, Relation r ) {
        // this class reads from it's Connector c's BufferedReader b
        this.c = c;  // Connector
        b = c.in;  // BufferedReader
    }

    // instance methods
    public Tuple getNextTuple() throws Exception {
        // convert next string in BufferedReader b to tuple
        String ts = b.readLine();
        if (ts == null) return null;
        Tuple t = Tuple.makeTupleFromPipeData(ts);
        return t;
    }

    public String getNextString() throws Exception {
        return b.readLine();
    }

    public Relation getRelation() {
        return c.getRelation();
    }

    public void setRelation( Relation r ) {
        c.setRelation(r);
    }

    public String getName() { return c.getName(); }

}
