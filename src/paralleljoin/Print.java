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
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author travisbrannen
 */
public class Print extends Thread implements GammaConstants{
    ReadEnd in;
    Relation r;  // required for formatting
    int pad = 3;  // number of extra spaces for columns in table
    
    public Print( Connector in, String relationName) {
        this.in = in.getReadEnd();
        r = Relation.GetRelationByName(relationName);
        // TODO: print table header
        ThreadList.add(this);
    }

    public void run() {

        LinkedList<Tuple> inputs = new LinkedList<Tuple>();   
        Tuple input;
        try {
            while (true) {
                input = in.getNextTuple();
                
                if (input == null) {
                    break;
                }
                
                inputs.add(input);
            }
        } catch (Exception e) {
            ReportError.msg(this.getClass().getName() + e);
        }
        System.out.flush();

        String[][] table = new String[inputs.size()+1][r.getSize()];
        for (int i=0; i<r.getSize(); i++){  // add column titles to table
            table[0][i] = r.getField(i);
        }
        for (int i=0; i<inputs.size(); i++){  // add table entries to table
            for (int j=0; j<r.getSize(); j++){
                table[i+1][j] = inputs.get(i).get(j);
            }
        }
        System.out.println("");
        printTable(table);
        System.out.println("");
    }
    
    public void printTable(String[][] table){
        // assuming top row of table is titles
        int[] maxLength;
        // figure out width of each column for table
        maxLength = new int[table[0].length];  // num columns
        for (int i=0; i<table[0].length; i++){
            maxLength[i] = 0;
        }
        for (int i=0; i<table.length; i++){  // for each row i
            for (int j=0; j<table[i].length; j++){  // for each col j, cell(i,j)
                if (table[i][j].length()>maxLength[j]){
                    maxLength[j] = table[i][j].length();
                }
            }
        }
        for (int i=0; i<table[0].length; i++){
            maxLength[i] = maxLength[i] + pad;  // pad columns
        }
        for (int i=0; i<table.length; i++){  // print table
            if (i==1){  // line under first row
                for (int j=0; j<table[i].length; j++){
                    for (int l=0; l<maxLength[j]; l++){
                        if (l==maxLength[j] - 1){
                            System.out.print(" ");
                        }
                        else{
                            System.out.print("-");
                        }
                    }
                }
                System.out.println("");
            }
            for (int j=0; j<table[i].length; j++){  // write words in table
                System.out.print(table[i][j]);
                for (int k=0; k<(maxLength[j] - table[i][j].length()); k++){
                    System.out.print(" ");
                }
            }
            System.out.println("");
        }
    }
    
    public void printTuple(String tupleString) throws Exception{
        // TODO: reformat print to look good with header
        Tuple t = Tuple.makeTupleFromPipeData(tupleString);
        t.println(r);
    }
}
