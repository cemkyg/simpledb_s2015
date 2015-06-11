package simpledb.index.query;

import cengiz.LogMan;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.metadata.IndexInfo;
import simpledb.query.*;
import simpledb.index.Index;

import java.util.ArrayList;
import java.util.logging.Logger;

/** The Plan class corresponding to the <i>indexjoin</i>
  * relational algebra operator.
  * @author Edward Sciore
  */
public class MultipleIndexJoinPlan implements Plan {
   private static Logger logger = LogMan.getLogger();

   private Plan p1, p2;
   private ArrayList<IndexInfo> iis;
   private ArrayList<String> joinfs;
   private Schema sch = new Schema();
   
   /**
    * Implements the join operator,
    * using the specified LHS and RHS plans.
    * @param p1 the left-hand plan
    * @param p2 the right-hand plan
    * @param iis information about the right-hand index
    * @param joinfs the left-hand field used for joining
    * @param tx the calling transaction
    */
   public MultipleIndexJoinPlan(Plan p1, Plan p2, ArrayList<IndexInfo> iis, ArrayList<String> joinfs, Transaction tx) {
      //logger.info(String.format("MultipleIndexJoinPlan adayi, %s uzerindeki indexten %s'e.", ii.getFldname(), joinfield));

      this.p1 = p1;
      this.p2 = p2;
      this.iis = iis;
      this.joinfs = joinfs;
      sch.addAll(p1.schema());
      sch.addAll(p2.schema());
   }
   
   /**
    * Opens an indexjoin scan for this query
    * @see simpledb.query.Plan#open()
    */
   public Scan open() {
      Scan s = p1.open();
      // throws an exception if p2 is not a tableplan
      TableScan ts = (TableScan) p2.open();

      ArrayList<Index> idxs = new ArrayList<Index>();

      for (IndexInfo ii : iis) {
         idxs.add(ii.open());
      }

      return new MultipleIndexJoinScan(s, idxs, joinfs, ts);
   }
   
   /**
    * Estimates the number of block accesses to compute the join.
    * The formula is:
    * <pre> B(indexjoin(p1,p2,idx)) = B(p1) + R(p1)*B(idx)
    *       + R(indexjoin(p1,p2,idx) </pre>
    * @see simpledb.query.Plan#blocksAccessed()
    */
   public int blocksAccessed() {
      return p1.blocksAccessed() 
         + (p1.recordsOutput() * iis.get(0).blocksAccessed())
         + recordsOutput();  // FIXME
   }
   
   /**
    * Estimates the number of output records in the join.
    * The formula is:
    * <pre> R(indexjoin(p1,p2,idx)) = R(p1)*R(idx) </pre>
    * @see simpledb.query.Plan#recordsOutput()
    */
   public int recordsOutput() {
      return p1.recordsOutput() * iis.get(0).recordsOutput(); // FIXME
   }
   
   /**
    * Estimates the number of distinct values for the 
    * specified field.  
    * @see simpledb.query.Plan#distinctValues(java.lang.String)
    */
   public int distinctValues(String fldname) {
      if (p1.schema().hasField(fldname))
         return p1.distinctValues(fldname);
      else
         return p2.distinctValues(fldname);
   }
   
   /**
    * Returns the schema of the index join.
    * @see simpledb.query.Plan#schema()
    */
   public Schema schema() {
      return sch;
   }

   public int getRDF() {
      logger.severe("Burada RDF cagirilmamali.");
      return 1;
   }

//   public String toString() {
//      String retval = String.format("Solda: (%s), Sagda: (%s), Indexler: \n", p1.toString(), p2.toString());
//      for (IndexInfo ii : iis) {
//         retval += ii.toString() + "\n";
//      }
//      return retval;
//   }
}
