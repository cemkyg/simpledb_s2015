package cengiz;

import java.sql.*;
import simpledb.remote.SimpleDriver;

public class InsertClient {

   private static String[] generate_record1(int size) {
      String[] recs = new String[size];

      for (int i=0; i<size; i++) {
         recs[i] = String.format("(%d, %d, %d)", i, i, i);
      }
      return recs;
   }

   public static void main(String[] args) {
      Connection conn = null;
      try {
         Driver d = new SimpleDriver();
         conn = d.connect("jdbc:simpledb://localhost", null);
         Statement stmt = conn.createStatement();

         String table1 = "create table A(a1 int, a2 int, a3 int)";
         String table1insert = "insert into A(a1, a2, a3) values ";
         String[] table1indexes = {
               "create index idxa1 on A(a1)",
               "create index idxa2 on A(a2)",
         };
         String[] table1vals = generate_record1(5);

         String table2 = "create table B(b1 int, b2 int, b3 int)";
         String table2insert = "insert into B(b1, b2, b3) values ";
         String[] table2indexes = {
               "create index idxb1 on B(b1)"
         };
         String[] table2vals = {
               "(1, 11, 11)",
               "(2, 22, 22)",
               "(3, 33, 33)",
               "(44, 44, 44)",
               "(55, 55, 55)"
         };

         String table3 = "create table C(c1 int, c2 int, c3 int)";
         String table3insert = "insert into C(c1, c2, c3) values ";
         String[] table3vals = {
               "(1, 1, 1)"
         };

         String table4 = "create table D(d1 varchar(25), d2 varchar(25), d3 varchar(25))";
         String table4insert = "insert into D(d1, d2, d3) values ";
         String[] table4vals = {
               "('abc', 'cde', 'efg')"
         };
         
         System.out.println("Create table1");
         stmt.executeUpdate(table1);

         System.out.println("Put table1 index");
         for (String idx : table1indexes)
            stmt.executeUpdate(idx);

         System.out.println("Put table1 values");
         for (String val : table1vals)
            stmt.executeUpdate(table1insert + val);


         System.out.println("Create table2");
         stmt.executeUpdate(table2);

         System.out.println("Put table2 index");
         for (String idx : table2indexes)
            stmt.executeUpdate(idx);

         System.out.println("Put table2 values");
         for (String val : table2vals)
            stmt.executeUpdate(table2insert + val);


         System.out.println("Create table3");
         stmt.executeUpdate(table3);

         System.out.println("Put table3 values");
         for (String val : table3vals)
            stmt.executeUpdate(table3insert + val);

         System.out.println("Create table4");
         stmt.executeUpdate(table4);

         System.out.println("Put table4 values");
         for (String val : table4vals)
            stmt.executeUpdate(table4insert + val);


      } catch (SQLException e) {
         System.out.println("Olmadi.");
         e.printStackTrace();
      } finally {
         try {
            if (conn != null) {
               conn.close();
            }
         } catch (SQLException e) {
            System.out.println("Kapatirken olmadi.");
            e.printStackTrace();
         }
      }
   }
}
