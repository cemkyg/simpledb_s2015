package cengiz;

import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;


public class LogMan {
   private static Logger logger;

   public static Logger getLogger() {
      if (logger == null) {
         logger = Logger.getLogger("my.logger");
         logger.setLevel(Level.FINE);
      }
      return logger;
   }
}
