package simpledb.index.query;

import cengiz.LogMan;
import simpledb.record.RID;
import simpledb.query.*;
import simpledb.index.Index;

import java.util.ArrayList;
import java.util.logging.Logger;

/**
 * The scan class corresponding to the select relational
 * algebra operator.
 * @author Edward Sciore
 */
public class MultipleIndexSelectScan implements Scan {
   private static Logger logger = LogMan.getLogger();

   private ArrayList<Index> idxs;
   private ArrayList<Constant> vals;
   private ArrayList<RID> rids;
   private TableScan ts;

   private int iterstate;

   
   /**
    * Creates an index select scan for the specified
    * index and selection constant.
    * @param idx the index
    * @param val the selection constant
    */
   public MultipleIndexSelectScan(ArrayList<Index> idxs, ArrayList<Constant> vals, TableScan ts) {
      // TODO: Birden fazla index atilirsa OLAYINI ACIKLA!
      assert idxs.size() == vals.size();

      this.idxs = idxs;
      this.vals = vals;
      this.ts  = ts;
      beforeFirst();
   }
   
   /**
    * Positions the scan before the first record,
    * which in this case means positioning the index
    * before the first instance of the selection constant.
    * @see simpledb.query.Scan#beforeFirst()
    */
   public void beforeFirst() {
      this.iterstate = 0;
      for (int i=0; i<idxs.size(); i++) {
         idxs.get(i).beforeFirst(vals.get(i));
      }
      readIndexes();  // <-- FIXME?: Superhack
   }
   
   /**
    * Moves to the next record, which in this case means
    * moving the index to the next record satisfying the
    * selection constant, and returning false if there are
    * no more such index records.
    * If there is a next record, the method moves the 
    * tablescan to the corresponding data record.
    * @see simpledb.query.Scan#next()
    */
   public boolean next() {
      boolean ok = iterstate < rids.size();
      if (ok) {
         RID rid = rids.get(iterstate);
         ts.moveToRid(rid);
         iterstate++;
      }
      return ok;
   }
   
   /**
    * Closes the scan by closing the index and the tablescan.
    * @see simpledb.query.Scan#close()
    */
   public void close() {
      for (Index i : idxs)
         i.close();
      ts.close();
   }
   
   /**
    * Returns the value of the field of the current data record.
    * @see simpledb.query.Scan#getVal(java.lang.String)
    */
   public Constant getVal(String fldname) {
      return ts.getVal(fldname);
   }
   
   /**
    * Returns the value of the field of the current data record.
    * @see simpledb.query.Scan#getInt(java.lang.String)
    */
   public int getInt(String fldname) {
      return ts.getInt(fldname);
   }
   
   /**
    * Returns the value of the field of the current data record.
    * @see simpledb.query.Scan#getString(java.lang.String)
    */
   public String getString(String fldname) {
      return ts.getString(fldname);
   }
   
   /**
    * Returns whether the data record has the specified field.
    * @see simpledb.query.Scan#hasField(java.lang.String)
    */
   public boolean hasField(String fldname) {
      return ts.hasField(fldname);
   }

   private void readIndexes() {
      // Ne kadar index tanimlanmis ise hepsini oku, hepsinin RID'leri esit olanlar varsa genel havuza at.
      ArrayList<ArrayList<RID>> ridcont = new ArrayList<ArrayList<RID>>();

      for (Index idx : idxs)
         ridcont.add(new ArrayList<RID>());

      for (int i=0; i<ridcont.size(); i++) {
         ArrayList<RID> ridc = ridcont.get(i);
         Index idx = idxs.get(i);

         while (idx.next()) {
            ridc.add(idx.getDataRid());
            logger.fine(String.format("Got RID: %s", idx.getDataRid().toString()));
         }
      }

      rids = ridcont.get(0);
      for (int i=1; i<ridcont.size(); i++) {
         rids = joinLists(rids, ridcont.get(i));
      }

      logRIDS();
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
      logger.fine("Logging RIDS");
      for (RID rid : rids)
         logger.fine(rid.toString());
   }


}
