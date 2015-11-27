/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package paralleljoin;

import java.io.BufferedReader;
import java.io.PrintStream;
import java.io.FileInputStream;
import java.io.InputStreamReader;

import gammaSupport.*;

import basicConnector.*;
import java.util.StringTokenizer;
/**
 *
 * @author travisbrannen
 */
public class ReadRelation extends Thread implements GammaConstants{
    BufferedReader in;
    Relation r;
    WriteEnd w;
    
    public ReadRelation(String relationName, Connector out) throws Exception{
        this(Rel + "/"  + relationName+ ".txt", relationName, out);
    }
    
    //  Initializes Read Relation object, creates relation but doesn't fill it
    public ReadRelation(String filename, String relationName, Connector out)
                                                            throws Exception{
        // Step 0:  remember write end
        w = out.getWriteEnd();
        // Step 1: open data file and create relation
        in = new BufferedReader(
                new InputStreamReader(new FileInputStream(filename)));
        String line = in.readLine();  // gotta be ready
        r = createRelation(line, relationName);
        // Step 2: skip ---- line
        in.readLine();  // just getting the underlines out of the way
        // Step 3: ready to read data
        ThreadList.add(this);
    }
    
    // Creates a Relation object whose name is "relationName"
    // from the first line read in a data file.
    static  Relation createRelation( String line, String relationName ) {
        // Step 1: count the number of fields (attributes) in relation
        StringTokenizer st = new StringTokenizer(line);
        int size = st.countTokens();
        // Step 2: create relation
        Relation r = new Relation( relationName, size );
        // Step 3: populate fieldnames (attribute names)
        for (int i=0; i<size; i++){
            r.addField(st.nextToken());
        }
        return r;
    }
    
    @Override
    public void run() {
        try {
            while(true) {
                Tuple t = getNextTuple();
                if (t==null) {
                    w.close();
                    break;
                } else
                    w.putNextTuple(t);
            }
        } catch(Exception e) { ReportError.msg(this, e); }
    }

    public Tuple getNextTuple() throws Exception {        
        String s = in.readLine();
        
        if (s.isEmpty()){
            return null;
        }
        Tuple t = Tuple.makeTupleFromFileData(r, s);
        return t;
    }
    
}