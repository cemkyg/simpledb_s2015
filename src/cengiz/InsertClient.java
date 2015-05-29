package cengiz;

import java.sql.*;
import simpledb.remote.SimpleDriver;

public class InsertClient {
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
         };
         String[] table1vals = {
               "(1, 1, 1)",
               "(2, 2, 2)",
               "(3, 3, 3)",
               "(4, 4, 4)",
               "(5, 5, 5)"
         };

         String table2 = "create table B(b1 int, b2 int, b3 int)";
         String table2insert = "insert into B(b1, b2, b3) values ";
         String[] table2indexes = {
               "create index idxb1 on B(b1)"
         };
         String[] table2vals = {
               "(1, 1, 1)",
               "(2, 2, 2)",
               "(3, 3, 3)",
               "(4, 4, 4)",
               "(5, 5, 5)"
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
