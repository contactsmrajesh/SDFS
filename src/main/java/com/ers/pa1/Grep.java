package com.ers.pa1;

import java.io.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Grep implements the REGEX Grep functionality to search
 * the pattern queried by the user with in the log file; returns
 * all the found and matched pattern lines as a result by considering the
 * options per user's input.
 */
public class Grep {

    /**
     * This method is intended to match the search pattern per user's query
     *
     * @return Matched lines from the log file
     * @throws IOException
     * @params s : search string/regex pattern per user's query
     * @params ignoreCase : CaseInsensitive search option
     * @params reverseMatch : Match invert option
     * @params printLineNum : option to print line number
     */
    public String calPattern(String s, String ignoreCase, String reverseMatch, String printLineNum) {

        StringBuilder sb = new StringBuilder();
        String res = "";
        Pattern sre = null;

        if (ignoreCase.equals("Y")) {
            sre = Pattern.compile(s, Pattern.CASE_INSENSITIVE);
        } else if (ignoreCase.equals("N")) {
            sre = Pattern.compile(s);
        }

        BufferedReader br = null;
        int counter = 0;
        try {
            //FileInputStream fstream = new FileInputStream("C:\\Users\\raj\\Desktop\\test\\machine.i.log");
            FileInputStream fstream = new FileInputStream("/home/ec2-user/logs/Application.log");
            DataInputStream inp = new DataInputStream(fstream);
            br = new BufferedReader(new InputStreamReader(inp));
        } catch (FileNotFoundException e) {
            System.err.println("Unable to open file :" + e.getMessage());
            System.exit(1);
        }

        try {
            String st;
            while ((st = br.readLine()) != null) {
                counter++;
                String printLine = (printLineNum.equals("Y")) ? (String.valueOf(counter) + ":") : "";

                Matcher m = sre.matcher(st);
                boolean isFound = m.find();
                if (isFound && reverseMatch.equals("N")) {
                    sb.append(printLine + " " + st);
                    sb.append(System.getProperty("line.separator"));
                } else if ((!(isFound)) && reverseMatch.equals("Y")) {
                    sb.append(printLine + " " + st);
                    sb.append(System.getProperty("line.separator"));
                    //sb.append("\n");
                }
            }
            res = sb.toString().trim();

        } catch (Exception e) {
            System.err.println(" reading line: " + e.getMessage());
            System.exit(1);
        }
        return res;
    }
}



