package cengiz;

import simpledb.remote.SimpleDriver;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.Statement;

public class QueryClient {
   public static void main(String[] args) {
      Connection conn = null;
      try {
         Driver d = new SimpleDriver();
         conn = d.connect("jdbc:simpledb://localhost", null);

         Statement stmt = conn.createStatement();
          String query = "select a1, a2, b1, b2 from A, B where a1 = b1 and a2 = b2 and a3 = b3";
         //String query = "select a1, a2 from A where a1 = 1 and a2 = 1";

         ResultSet rs = stmt.executeQuery(query);

         while (rs.next()) {
             System.out.println(String.format("%d %d %d %d", rs.getInt("a1"), rs.getInt("a2"), rs.getInt("b1"), rs.getInt("b2")));
            //System.out.println(String.format("%d %d", rs.getInt("a1"), rs.getInt("a2")));
         }
         rs.close();
      } catch (Exception e) {
         e.printStackTrace();
      } finally {
         try {
            if (conn != null)
               conn.close();
         } catch (Exception e) {
            e.printStackTrace();
         }
      }

   }
}
