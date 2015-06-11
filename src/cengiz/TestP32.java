package cengiz;

import java.sql.*;
import simpledb.remote.SimpleDriver;

/**
 * Buradaki kod HeuristicPlannner3 icin istenilenleri test ediyor.
 */
public class TestP32 {

   public static Driver d;
   public static Connection conn;
   public static Statement stmt;

   public static void testp32_1() {
      // Bunu nasil test edecegimi bilemedim, ancak implementasyonu anlatmak hayli kolay.
      // HeuristicQueryPlanner'in icerisinde "getLowest...Plan" metodlari var. Bu metodlar aday plan olusturup, maliyet
      // kontrolu yapmaktalar.

      // Halihazirda bu kontrolu
      //    plan.recordsOutput() < bestplan.recordsOutput()
      // satiri ile yapmaktaydilar.

      // Ben bu satiri
      //    plan.blocksAccessed < bestplan.blocksAccessed()
      // seklinde degistirdim.

      System.out.println("Proje 3'un ilk adimi icin test yok, gerekli bilgi icin lutfen bu print fonksiyonunun" +
            " cagirildigi yerdeki aciklamalari okuyun");
   }


   public static void createTables2() throws Exception {
      System.out.println("Creating tables for p32_2");

      String table1 = "create table inttable(fint1 int, fint2 int)";
      String table2 = "create table strtable(fstr1 varchar(25), fstr2 varchar(25))";

      stmt.executeUpdate(table1);
      stmt.executeUpdate(table2);
   }

   public static void insertRecords2() throws Exception {
      // inttable buyuklugu ~8
      // strtable buyuklugu ~50
      // Blok buyuklugu standard olarak 400. Biz 3 blok dolduralim.

      // inttable'de blok basina 50 kayit, strtablede blok basina 8 kayit
      // O zaman inttable'ye 125, strtable'ye 20 kayit ekleyelim.

      System.out.println("Inserting data for P32_2");

      for (int i=0; i<125; i++) {
         String insertstmt = String.format("insert into inttable(fint1, fint2) values (%d, %d)", i, 500-i);
         stmt.executeUpdate(insertstmt);
      }

      for (int i=0; i<20; i++) {
         String insertstmt = String.format("insert into strtable(fstr1, fstr2) values ('%s', '%s')",
               String.format("Ali %d top al", i),
               String.format("Ayse %d lastik al", 500-i));
         stmt.executeUpdate(insertstmt);
      }
   }

   public static void insertData2() throws Exception {
      createTables2();
      insertRecords2();
   }

   public static void queryData2() throws Exception {
      System.out.println("Executing query 1");
      String querystr = "select fint1, fstr1 from inttable, strtable";
      stmt.executeQuery(querystr);
   }

   public static void testp32_2() throws Exception {
      // Burada istenilen iki tabloyu join ederken maliyet hesabini gozonunde bulundurarak tablolarin siralarini degistirmek.
      // Bunu soyle gozlemleyebiliriz.

      // Iki tane tablomuz olacak. Bu tablolari esit sayida blok dolduracak sekilde kayit ekleyecegiz, ancak tablolardan
      // bir tanesinin kayit buyuklugu yuksek olacak.

      // B sayilari esitken, planlayici RPB degeri dusuk olan tabloyu sol tarafa koymali, bizim durumumuzda da bu strtable oluyor.
      // Bu kararin verildigini, simpledb'nin calistigi yerde log kayitlari araciligi ile gorebilirsiniz.


      insertData2();
      queryData2();
   }


   public static void createTables3() throws Exception {
      System.out.print("Creating tables for P32_3...");

      String table1 = "create table arjantin(arj1 int, arj2 int, arj3 varchar(30))";
      stmt.executeUpdate(table1);

      String table2 = "create table brezilya(bre1 int, bre2 int, bre3 int)";
      stmt.executeUpdate(table2);

      System.out.println("Done");
   }

   public static void insertRecords3() throws Exception {
      // arjantin buyuklugu ~38
      // brezilya buyuklugu ~12

      // arjantine 100 kayit ekleyelim.
      // brezilyaya 350 kayit ekleyelim.

      System.out.print("Inserting data for P32_3... ");

      for (int i=0; i<100; i++) {
         String insertstmt = String.format("insert into arjantin(arj1, arj2, arj3) values (%d, %d, '%s')",
               i, 1000-i,
               String.format("Messi %d gol atti.", i));
         stmt.executeUpdate(insertstmt);
      }

      for (int i=0; i<350; i++) {
         String insertstmt = String.format("insert into brezilya(bre1, bre2, bre3) values (%d, %d, %d)",
               i, 500-i, 3000-i);
         stmt.executeUpdate(insertstmt);
      }

      stmt.executeUpdate("create index idxarj1 on arjantin(arj1)");
      stmt.executeUpdate("create index idxbre1 on brezilya(bre1)");
      stmt.executeUpdate("create index idxarj2 on arjantin(arj2)");
      stmt.executeUpdate("create index idxbre2 on brezilya(bre2)");


      System.out.println("Done");

   }

   public static void insertData3() throws Exception {
      createTables3();
      insertRecords3();
   }

   public static void queryData3() throws Exception {
      String querystr = "select arj1, bre1, arj2, bre2 from brezilya, arjantin where arj1 = bre1 and arj2 = bre2";
      stmt.executeQuery(querystr);
   }

   public static void testp32_3() throws Exception {
      // Buradaki durumu gostermek icin testp32_2'deki gibi bir ayarlama yapamadim. Yapilamaz degil ama cok ince ayar
      // yapmak gerekiyor.

      // Yine farkli buyuklukte, asagi yukari ayni blok sayisi olacak sekilde iki tablo olusturup bunlari ikisi uzerinde
      // de index olan bir alan uzerinden join yaptim.

      insertData3();
      queryData3();
   }

   public static void init() throws Exception {
      d = new SimpleDriver();
      conn = d.connect("jdbc:simpledb://localhost", null);
      stmt = conn.createStatement();
   }

   public static void release() throws Exception {
      conn.close();
   }

   public static void main(String[] args) throws Exception {
      init();
      //testp32_1();
      //testp32_2();
      //testp32_3();
      release();

      System.out.println("Bitti");
   }
}
