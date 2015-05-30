package simpledb.index.query;

import cengiz.LogMan;
import simpledb.metadata.StatInfo;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.metadata.IndexInfo;
import simpledb.query.*;
import simpledb.index.Index;

import java.util.ArrayList;
import java.util.logging.Logger;

/** The Plan class corresponding to the <i>indexselect</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class MultipleIndexSelectPlan implements Plan {
   private Plan p;
   private ArrayList<IndexInfo> iis;
   private ArrayList<Constant> vals;
   private Logger logger;

   /**
    * Creates a new indexselect node in the query tree
    * for the specified index and selection constant.
    * @param p the input table
    * @param ii information about the index
    * @param val the selection constant
    * @param tx the calling transaction 
    */
   public MultipleIndexSelectPlan(Plan p, ArrayList<IndexInfo> ii, ArrayList<Constant> val, Transaction tx) {
      this.p = p;
      this.iis = ii;
      this.vals = val;
      logger = LogMan.getLogger();
   }
   
   /** 
    * Creates a new indexselect scan for this query
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      // throws an exception if p is not a tableplan.
      TableScan ts = (TableScan) p.open();
      ArrayList<Index> idxs = new ArrayList<Index>();

      for (IndexInfo ii : iis)
         idxs.add(ii.open());

      return new MultipleIndexSelectScan(idxs, vals, ts);
   }
   
   /**
    * Estimates the number of block accesses to compute the 
    * index selection, which is the same as the 
    * index traversal cost plus the number of matching data records.
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      int total = 0;
      for (IndexInfo ii : iis)
         total += ii.blocksAccessed();
      total += recordsOutput();
      return total;
   }
   
   /**
    * Estimates the number of output records in the index selection,
    * which is the same as the number of search key values
    * for the index.
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      // Ana mantik; 1 index 5 dusuruyorsa 2 index 25 dusurur. Geometrik artim.
      // Ne kadar index varsa (pred.reduction_factor)^index_len hesapla. En sonu da toplam_kayit/geometrik_rf

      // Burada statinfo_ya ihtiyacimiz var. Bunu adam akilli yollardan elde etmek cok mesakkatli geldi, o nedenden
      // dolayi "hack"imsi bir yola basvuruyorum.  Boyle yapiyoruz ama butun indexlerin ayni tablo icin gelip gelme-
      // digini de bi kontrol edelim.
      StatInfo si = iis.get(0).getStatInfo();
      for (IndexInfo ii : iis) {
         if (si != ii.getStatInfo()) {
            logger.severe("Indexler ayni yerden GELMIYOR!");
            throw new RuntimeException();
         } else {
            logger.info("Indexler ayni yerden geliyor.");
         }
      }

      int finalrecout = si.recordsOutput();
      for (IndexInfo ii : iis)
         finalrecout /= ii.getDistinctValues();
      return finalrecout;
   }
   
   /** 
    * Returns the distinct values as defined by the index.
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      // Normal dagilim altinda,
      // Eger fldname'nin uzerinde index varsa 1 gelir. Select atiyoruz nitekim.
      // Degilse, IndexInfo'daki formulun aynisini caksin.
      for (IndexInfo ii : iis) {
         if (ii.getFldname().equals(fldname))
            return 1;
      }
      return Math.min(iis.get(0).getStatInfo().distinctValues(fldname), recordsOutput());
   }
   
   /**
    * Returns the schema of the data table.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return p.schema(); 
   }

   public int getRDF() {
      logger.severe("Burada RDF cagirilmamali.");
      return 1;
   }

}
