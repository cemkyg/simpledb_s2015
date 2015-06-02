package simpledb.opt;

import cengiz.LogMan;
import simpledb.tx.Transaction;
import simpledb.record.Schema;
import simpledb.query.*;
import simpledb.index.query.*;
import simpledb.metadata.IndexInfo;
import simpledb.multibuffer.MultiBufferProductPlan;
import simpledb.server.SimpleDB;

import java.util.ArrayList;
import java.util.Map;
import java.util.logging.Logger;

/**
 * This class contains methods for planning a single table.
 * @author Edward Sciore
 */
class TablePlanner {
   private static Logger logger = LogMan.getLogger();

   private TablePlan myplan;
   private Predicate mypred;
   private Schema myschema;
   private Map<String,IndexInfo> indexes;
   private Transaction tx;
   
   /**
    * Creates a new table planner.
    * The specified predicate applies to the entire query.
    * The table planner is responsible for determining
    * which portion of the predicate is useful to the table,
    * and when indexes are useful.
    * @param tblname the name of the table
    * @param mypred the query predicate
    * @param tx the calling transaction
    */
   public TablePlanner(String tblname, Predicate mypred, Transaction tx) {
      this.mypred  = mypred;
      this.tx  = tx;
      myplan   = new TablePlan(tblname, tx);
      myschema = myplan.schema();
      indexes  = SimpleDB.mdMgr().getIndexInfo(tblname, tx);
   }
   
   /**
    * Constructs a select plan for the table.
    * The plan will use an indexselect, if possible.
    * @return a select plan for the table.
    */
   public Plan makeSelectPlan() {
      Plan p = makeIndexSelect();
      if (p == null)
         p = myplan;
      return addSelectPred(p);
   }
   
   /**
    * Constructs a join plan of the specified plan
    * and the table.  The plan will use an indexjoin, if possible.
    * (Which means that if an indexselect is also possible,
    * the indexjoin operator takes precedence.)
    * The method returns null if no join is possible.
    * @param current the specified plan
    * @return a join plan of the plan and this table
    */
   public Plan makeJoinPlan(Plan current) {
      Schema currsch = current.schema();
      Predicate joinpred = mypred.joinPred(myschema, currsch);
      if (joinpred == null)
         return null;
      Plan p = makeIndexJoin(current, currsch);
      if (p == null)
         p = makeProductJoin(current, currsch);
      return p;
   }
   
   /**
    * Constructs a product plan of the specified plan and
    * this table.
    * @param current the specified plan
    * @return a product plan of the specified plan and this table
    */
   public Plan makeProductPlan(Plan current) {
      // Product sirasinin duzeltilmesi bu fonksiyon altinda oluyor.
      // Basitce, verilen plan ile yaratilmis planlarin yerlerini degistirip bir de oyle bakiyoruz. Hangisi daha az
      // maliyetli ise onu donduruyoruz.
      Plan p = addSelectPred(myplan);

      Plan plan1 = new MultiBufferProductPlan(current, p, tx);
      Plan plan2 = new MultiBufferProductPlan(p, current, tx);

      logger.info("plan1 maliyet: " + plan1.blocksAccessed());
      logger.info("plan2 maliyet: " + plan2.blocksAccessed());

      if (plan1.blocksAccessed() < plan2.blocksAccessed()) {
         logger.info("makeProductPlan orijinal planda karar kildi");
         return plan1;
      }
      else {
         logger.info("makeProductPlan diger planda karar kildi");
         return plan2;
      }
   }
   
   private Plan makeIndexSelect() {
      for (String fldname : indexes.keySet()) {
         Constant val = mypred.equatesWithConstant(fldname);
         if (val != null) {
            IndexInfo ii = indexes.get(fldname);
            return new IndexSelectPlan(myplan, ii, val, tx);
         }
      }
      return null;
   }
   
   private Plan makeIndexJoin(Plan current, Schema currsch) {
      // Ikinci kismin index problemi burada yapiliyor.
      // Kisaca, iki tarafin da butun indexlerini topluyoruz. Hepsi icin plan olusturuyoruz, en kucugunu aliyoruz.

      // Burada "current" sol taraftan gelen plan. Bu TablePlanner sag taraftaki tablo uzerine.
      // Sol taraftan sadece TablePlan geldigi zaman soldakinin de indexlerine bakalim diyecegiz.

      ArrayList<IndexOuterfieldWrapper> lhsii = gatherIndexes(current, currsch);
      ArrayList<IndexOuterfieldWrapper> rhsii = gatherIndexes(myplan, currsch);

      Plan bestplan = null;
      for (IndexOuterfieldWrapper iow : lhsii) {
         Plan canditate = new IndexJoinPlan(myplan, current, iow.ii, iow.outerfield, tx);
         if (bestplan == null || canditate.blocksAccessed() < bestplan.blocksAccessed()) {
            bestplan = canditate;
         }
      }

      for (IndexOuterfieldWrapper iow : rhsii) {
         Plan canditate = new IndexJoinPlan(current, myplan, iow.ii, iow.outerfield, tx);
         if (bestplan == null || canditate.blocksAccessed() < bestplan.blocksAccessed()) {
            bestplan = canditate;
         }
      }

      logFoundPlan((IndexJoinPlan) bestplan);

      if (bestplan == null)
         return null;  // Index yok.

      bestplan = addSelectPred(bestplan);
      return addJoinPred(bestplan, currsch);


//      for (String fldname : indexes.keySet()) {
//         String outerfield = mypred.equatesWithField(fldname);
//         if (outerfield != null && currsch.hasField(outerfield)) {
//            IndexInfo ii = indexes.get(fldname);
//            Plan p = new IndexJoinPlan(current, myplan, ii, outerfield, tx);
//            p = addSelectPred(p);
//            return addJoinPred(p, currsch);
//         }
//      }
//      return null;
   }
   
   private Plan makeProductJoin(Plan current, Schema currsch) {
      Plan p = makeProductPlan(current);
      return addJoinPred(p, currsch);
   }
   
   private Plan addSelectPred(Plan p) {
      Predicate selectpred = mypred.selectPred(myschema);
      if (selectpred != null)
         return new SelectPlan(p, selectpred);
      else
         return p;
   }
   
   private Plan addJoinPred(Plan p, Schema currsch) {
      Predicate joinpred = mypred.joinPred(currsch, myschema);
      if (joinpred != null)
         return new SelectPlan(p, joinpred);
      else
         return p;
   }

   private ArrayList<IndexOuterfieldWrapper> gatherIndexes(Plan p, Schema currsch) {
      ArrayList<IndexOuterfieldWrapper> retval = new ArrayList<IndexOuterfieldWrapper>();

      if (!(p instanceof TablePlan))
         return retval;  // Bos konteynir

      Map<String, IndexInfo> idxs = SimpleDB.mdMgr().getIndexInfo(((TablePlan) p).getTblname(), tx);

      for (String fldname : idxs.keySet()) {
         String outerfield = mypred.equatesWithField(fldname);
         if (outerfield != null && currsch.hasField(outerfield)) {
            logger.info(fldname + " uzerinde bir index bulduk");
            IndexOuterfieldWrapper newiow = new IndexOuterfieldWrapper();
            newiow.ii = idxs.get(fldname);
            newiow.outerfield = outerfield;
            retval.add(newiow);
         }
      }
      return retval;
   }

   private void logFoundPlan(IndexJoinPlan p) {
      logger.info(p.joinfield + "'e gidne bir yuklem uzerinde en iyi plan olusturuldu");
   }

}
