package cengiz;

import java.sql.*;
import simpledb.remote.SimpleDriver;


public class TestP31 {

   public static Driver d;
   public static Connection conn;
   public static Statement stmt;

   public static void init() throws Exception {
      d = new SimpleDriver();
      conn = d.connect("jdbc:simpledb://localhost", null);
      stmt = conn.createStatement();
   }

   public static void release() throws Exception {
      conn.close();
   }

   public static void createTables1() throws Exception {
      System.out.print("Creating tables for P31_1... ");

      String table1 = "create table portekiz(pr1 int, pr2 int)";
      String table2 = "create table ispanya(sp1 int, sp2 int)";

      stmt.executeUpdate(table1);
      stmt.executeUpdate(table2);

      System.out.println("Done");
   }

   public static void insertRecords1() throws Exception {

      System.out.print("Inserting records for P31_1... ");

      for (int i=0; i<100; i++) {
         String insertstmt1 = String.format("insert into portekiz(pr1, pr2) values (%d, %d)", i, 500-i);
         stmt.executeUpdate(insertstmt1);
      }

      for (int i=0; i<50; i++) {
         String insertstmt1 = "insert into ispanya(sp1, sp2) values (1, 1)";
         stmt.executeUpdate(insertstmt1);
      }

      System.out.println("Done");
   }

   public static void insertData1() throws Exception {
      createTables1();
      insertRecords1();
   }

   public static ResultSet queryData1() throws Exception {
      String qstr = "select sp1, pr1 from ispanya, portekiz where sp1 = 1 and pr1 = 10";
      return stmt.executeQuery(qstr);
   }

   public static void printQuery1(ResultSet rset) throws Exception {
      while (rset.next())
         System.out.printf("%d %d\n", rset.getInt("sp1"), rset.getInt("pr1"));

   }

   public static void test31_1() throws Exception {
      // Planlar arasinda karsilastirma su satir ile
      //    plan.recordsOutput() < bestplan.recordsOutput()
      // yapiliyordu. Benim yaptigim bu satirlari
      //    plan.getRDF > bestplan.getRDF
      // ile degistirmek oldu.

      // Bunun testini soyle yaptim;
      // 2 tabloyu birbirine join edecegiz, ancak select yuklemi ile bu isi yapacagiz.
      // RDF degeri yuksek olan planin "makeLowestSelectPlan"'dan cikmasini bekleyecegiz.

      // SimpleDB'de "distincValues"'in hesaplanmasi toplam kayitlarin 3'e bolunmesi ile elde ediliyor.
      // Biz de bu farki yakalamak icin farkli sayilarda kayda sahip 2 tablo kullanacagiz.

      insertData1();
      ResultSet rset = queryData1();
      printQuery1(rset);
   }

   public static void createTables2() throws Exception {
      System.out.print("Creating tables for P31_2... ");

      String table = "create table turkiye(tr1 int, tr2 int, tr3 int)";
      stmt.executeUpdate(table);

      System.out.println("Done");
   }

   public static void insertRecords2() throws Exception {

      System.out.print("Inserting data for P31_2... ");

      for (int i=0; i<100; i++) {
         String insertstmt = String.format("insert into turkiye(tr1, tr2, tr3) values (%d, %d, %d)", i, 500-i, 1000-i);
         stmt.executeUpdate(insertstmt);
      }

      stmt.executeUpdate("create index idxtr1 on turkiye(tr1)");
      stmt.executeUpdate("create index idxtr2 on turkiye(tr2)");
      stmt.executeUpdate("create index idxtr3 on turkiye(tr3)");

      System.out.println("Done");
   }

   public static void insertData2() throws Exception {
      createTables2();
      insertRecords2();
   }

   public static void queryData2() throws Exception {
      String qstr = "select tr1, tr2, tr3 from turkiye where tr1 = 0 and tr2 = 500 and tr3 = 1000";
      stmt.executeQuery(qstr);
   }

   public static void test31_2() throws Exception{
      // TODO: Join ile zenginlestirilebilir.
      // Duz mantik, uc alanli bir tablo koyuyoruz, ucune de index atiyoruz, sonra bu alanlar uzerinden bir query
      // atiyoruz.

      insertData2();
      queryData2();
   }

   public static void createTables3() throws Exception {
      String table1 = "create table almanya(al1 int, al2 int))";
      stmt.executeUpdate(table1);

      String table2 = "create table fransa(fr1 int, fr2 int))";
      stmt.executeUpdate(table2);

      stmt.executeUpdate("create index idxal1 on almanya(al1)");
      stmt.executeUpdate("create index idxal2 on almanya(al2)");
      stmt.executeUpdate("create index idxfr1 on fransa(fr1)");
      stmt.executeUpdate("create index idxfr2 on fransa(fr2)");
   }

   public static void insertRecords3() throws Exception {

      System.out.print("Inserting records for P31_3... ");

      for (int i=0; i<100; i++) {
         String insertstmt1 = String.format("insert into almanya(al1, al2) values (%d, %d)", i, 500-i);
         String insertstmt2 = String.format("insert into fransa(fr1, fr2) values (%d, %d)", i, 500-i);

         stmt.executeUpdate(insertstmt1);
         stmt.executeUpdate(insertstmt2);
      }

      System.out.println("Done");
   }

   public static void insertData3() throws Exception {
      createTables3();
      insertRecords3();
   }

   public static ResultSet queryData3() throws Exception {
      String qstr = "select al1, al2, fr1, fr2 from almanya, fransa where al1 = fr1 and al2 = fr2 and al1 = 50";
      return stmt.executeQuery(qstr);
   }

   public static void printQuery3(ResultSet rs) throws Exception {
      while (rs.next())
         System.out.printf("%d %d %d %d\n", rs.getInt("al1"), rs.getInt("al2"), rs.getInt("fr1"), rs.getInt("fr2"));
   }

   public static void test31_3() throws Exception {
      insertData3();
      ResultSet rset = queryData3();
      printQuery3(rset);
   }

   public static void main(String[] args) throws Exception {
      init();
      //test31_1();
      //test31_2();
      //test31_3();
      release();

      System.out.println("Bitti");
   }

}
