package simpledb.index.query;

import cengiz.LogMan;
import simpledb.query.*;
import simpledb.index.Index;
import simpledb.record.RID;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * The scan class corresponding to the indexjoin relational
 * algebra operator.
 * The code is very similar to that of ProductScan, 
 * which makes sense because an index join is essentially
 * the product of each LHS record with the matching RHS index records.
 * @author Edward Sciore
 */
public class MultipleIndexJoinScan implements Scan {
   private static Logger logger = LogMan.getLogger();

   private Scan s;
   private TableScan ts;  // the data table
   private ArrayList<Index> idxs;
   private ArrayList<String> joinfs;

   private ArrayList<RID> rids;
   private int iterstate;
   
   /**
    * Creates an index join scan for the specified LHS scan and 
    * RHS index.
    * @param s the LHS scan
    * @param idxs the RHS index
    * @param joinfield the LHS field used for joining
    */
   public MultipleIndexJoinScan(Scan s, ArrayList<Index> idxs, ArrayList<String> joinfs, TableScan ts) {
      // logger.info("MultipleIndexJoinScan acildi. Joinfield: " + joinfield);
      this.s = s;
      this.idxs  = idxs;
      this.joinfs = joinfs;
      this.ts = ts;
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record.
    * That is, the LHS scan will be positioned at its
    * first record, and the index will be positioned
    * before the first record for the join value.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      s.beforeFirst();
      s.next();
      resetIndex();
   }
   
   /**
    * Moves the scan to the next record.
    * The method moves to the next index record, if possible.
    * Otherwise, it moves to the next LHS record and the
    * first index record.
    * If there are no more LHS records, the method returns false.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      while (true) {
         if (iterstate < rids.size()) {
            ts.moveToRid(rids.get(iterstate));
            iterstate++;
            return true;
         }
         if (!s.next())
            return false;
         resetIndex();
      }
   }
   
   /**
    * Closes the scan by closing its LHS scan and its RHS index.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      s.close();
      for (Index idx : idxs)
         idx.close();
      ts.close();
   }
   
   /**
    * Returns the Constant value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      if (ts.hasField(fldname))
         return ts.getVal(fldname);
      else
         return s.getVal(fldname);
   }
   
   /**
    * Returns the integer value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public int getInt(String fldname) {
      if (ts.hasField(fldname))
         return ts.getInt(fldname);
      else  
         return s.getInt(fldname);
   }
   
   /**
    * Returns the string value of the specified field.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public String getString(String fldname) {
      if (ts.hasField(fldname))
         return ts.getString(fldname);
      else
         return s.getString(fldname);
   }
   
   /** Returns true if the field is in the schema.
     * @see simpledb.query.Scan#hasField(java.lang.String)
     */
   public boolean hasField(String fldname) {
      return ts.hasField(fldname) || s.hasField(fldname);
   }
   
   private void resetIndex() {
      ArrayList<Constant> csts = new ArrayList<Constant>();

      iterstate = 0;

      for (String joinfld : joinfs) {
         Constant ct = s.getVal(joinfld);
         logger.fine(String.format("Will read indexes for value %d", (Integer) ct.asJavaVal()));
         csts.add(s.getVal(joinfld));
      }

      for (int i=0; i<idxs.size(); i++) {
         Index idx = idxs.get(i);
         Constant searchKey = csts.get(i);
         idx.beforeFirst(searchKey);
      }

      readIndexes();
   }

   private void readIndexes() {
      ArrayList<ArrayList<RID>> ridcontainer = new ArrayList<ArrayList<RID>>();

      for (Index idx : idxs)
         ridcontainer.add(new ArrayList<RID>());

      for (int i=0; i<ridcontainer.size(); i++) {
         ArrayList<RID> ridc = ridcontainer.get(i);
         Index idx = idxs.get(i);

         while (idx.next()) {
            ridc.add(idx.getDataRid());
         }
      }

      rids = ridcontainer.get(0);
      for (int i=1; i<ridcontainer.size(); i++) {
         rids = joinLists(rids, ridcontainer.get(i));
      }
      //logRIDS();
   }

   private ArrayList<RID> joinLists(ArrayList<RID> first, ArrayList<RID> other) {
      ArrayList<RID> retval = (ArrayList<RID>) first.clone();
      for (RID rid : first) {
         if (!other.contains(rid)) {
            retval.remove(rid);
         }
      }
      return retval;
   }

   private void logRIDS() {
      logger.info("Logging RIDS of size: " + rids.size());
      for (RID rid : rids)
         logger.info(rid.toString());
   }


}
