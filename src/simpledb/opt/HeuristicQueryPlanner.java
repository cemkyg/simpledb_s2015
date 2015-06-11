package simpledb.opt;

import cengiz.LogMan;
import simpledb.tx.Transaction;
import simpledb.query.*;
import simpledb.opt.TablePlanner;
import simpledb.parse.QueryData;
import simpledb.planner.QueryPlanner;
import java.util.*;
import java.util.logging.Logger;

/**
 * A query planner that optimizes using a heuristic-based algorithm.
 * @author Edward Sciore
 */
public class HeuristicQueryPlanner implements QueryPlanner {
   private static Logger logger = LogMan.getLogger();

   private Collection<TablePlanner> tableplanners = new ArrayList<TablePlanner>();
   
   /**
    * Creates an optimized left-deep query plan using the following
    * heuristics.
    * H1. Choose the smallest table (considering selection predicates)
    * to be first in the join order.
    * H2. Add the table to the join order which
    * results in the smallest output.
    */
   public Plan createPlan(QueryData data, Transaction tx) {
      
      // Step 1:  Create a TablePlanner object for each mentioned table
      for (String tblname : data.tables()) {
         TablePlanner tp = new TablePlanner(tblname, data.pred(), tx);
         tableplanners.add(tp);
      }
      
      // Step 2:  Choose the lowest-size plan to begin the join order
      Plan currentplan = getLowestSelectPlan();
      
      // Step 3:  Repeatedly add a plan to the join order
      while (!tableplanners.isEmpty()) {
         Plan p = getLowestJoinPlan(currentplan);
         if (p != null)
            currentplan = p;
         else  // no applicable join
            currentplan = getLowestProductPlan(currentplan);
      }
      
      // Step 4.  Project on the field names and return
      return new ProjectPlan(currentplan, data.fields());
   }
   
   private Plan getLowestSelectPlan() {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeSelectPlan();

         logger.info("Aday plan: " + plan.toString());

         if (bestplan == null || plan.getRDF() > bestplan.getRDF()) {
            besttp = tp;
            bestplan = plan;
         }
      }

      logger.info("getLowestSelectPlan en iyi plan: " + bestplan.toString());

      tableplanners.remove(besttp);
      return bestplan;
   }
   
   private Plan getLowestJoinPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeJoinPlan(current);

         if (bestplan == null || plan.getRDF() > bestplan.getRDF()) {

            besttp = tp;
            bestplan = plan;
         }
      }

      if (bestplan != null) {
         tableplanners.remove(besttp);
         logger.info("En iyi plan: " + bestplan.toString());
      } else {
         logger.info("Join icin plan yapilamadi");
      }

      return bestplan;
   }
   
   private Plan getLowestProductPlan(Plan current) {
      TablePlanner besttp = null;
      Plan bestplan = null;
      for (TablePlanner tp : tableplanners) {
         Plan plan = tp.makeProductPlan(current);
         if (bestplan == null || plan.getRDF() > bestplan.getRDF()) {
            besttp = tp;
            bestplan = plan;
         }
      }

      if (bestplan == null) {
         logger.info("Product icin plan yapilamadi");
      } else {
         logger.info("En iyi plan: " + bestplan.toString());
      }

      tableplanners.remove(besttp);
      return bestplan;
   }
}
