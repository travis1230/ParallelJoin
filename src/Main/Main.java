/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Main;

import org.junit.runner.JUnitCore;

public class Main {
    
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        System.out.println();
        System.out.println("--------GAMMA PROJECT-----");
        System.out.println();
        System.out.println("Please see Output folder and .pdf for description of each test.");
        System.out.println();
        System.out.println("Running Regression Tests:");
        System.out.println();
        JUnitCore.main("test.paralleljoin.MainTest");
        System.out.println();
    }

}
