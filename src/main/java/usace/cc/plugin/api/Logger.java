package usace.cc.plugin.api;

import static net.logstash.logback.argument.StructuredArguments.keyValue;

//import java.time.LocalDate;
//import usace.cc.plugin.api.Error.LoggingLevel;
import org.slf4j.LoggerFactory;

public class Logger {
  
    private CcLoggingLevel loggerLevel;
    //private String sender;
    private final org.slf4j.Logger logger;

     private static enum CcLoggingLevel {
        FATAL("FATAL",99),
        ERROR("ERROR",70),
        WARN("WARN",60),
        INFO("INFO",50),
        MESSAGE("MESSAGE",40),
        ACTION("ACTION",30),
        DEBUG("DEBUG",1);

        private final String level;
        private final int levelNumber;

        CcLoggingLevel(String level, int levelNumber){
            this.level=level;
            this.levelNumber=levelNumber;
        }

        public static CcLoggingLevel getLevelFromValue(int value){
            for (CcLoggingLevel level:CcLoggingLevel.values()){
                if (value>=level.levelNumber){
                    return level;
                }
            }
            return CcLoggingLevel.ERROR;
        }
    }

    public Logger(String logname){
        //this.errorLevel = level;
        var logLevelString = System.getenv(EnvironmentVariables.CC_LOGGING_LEVEL);
        if (logLevelString==null){
            this.loggerLevel=CcLoggingLevel.ACTION;
        } else {
            try{
                var llval = Integer.parseInt(logLevelString);
                this.loggerLevel=CcLoggingLevel.getLevelFromValue(llval);
            } catch(Exception ex){
                this.loggerLevel=CcLoggingLevel.ACTION;
            }
        }
        this.logger=LoggerFactory.getLogger(logname);
    }

    public void info(String msg, Object ...kvps){
        if (this.loggerLevel.levelNumber>=CcLoggingLevel.INFO.levelNumber){
            kvps=append(kvps, new Object[]{"level","INFO"});
            this.log(msg,kvps);
        }
    }

    public void debug(String msg, Object ...kvps){
        if (this.loggerLevel.levelNumber>=CcLoggingLevel.DEBUG.levelNumber){
            kvps=append(kvps, new Object[]{"level","DEBUG"});
            this.log(msg,kvps);
        }
    }

    public void warn(String msg, Object ...kvps){
        if (this.loggerLevel.levelNumber>=CcLoggingLevel.WARN.levelNumber){
            kvps=append(kvps, new Object[]{"level","WARN"});
            this.log(msg,kvps);
        }
    }

    public void error(String msg, Object ...kvps){
        if (this.loggerLevel.levelNumber>=CcLoggingLevel.ERROR.levelNumber){
            kvps=append(kvps, new Object[]{"level","ERROR"});
            this.log(msg,kvps);
        }
    }

    // public void action(String msg, Object ...kvps){
    //     kvps=append(kvps, new Object[]{"level","ACTION"});
    //     this.log(msg,kvps);
    // }

    public void sendMessage(String msg, Object ...kvps){
        if (this.loggerLevel.levelNumber>=CcLoggingLevel.MESSAGE.levelNumber){
            kvps=append(kvps, new Object[]{"level","MESSAGE"});
            this.log(msg,kvps);
        }
    }

    private void log(String msg, Object ...kvps){
        if (kvps.length%2!=0){
            throw new IllegalArgumentException("invalid key value pair objects, length is not an even number");
        }
        var kv = new Object[kvps.length/2];

        for (int i=0;i<kvps.length;i+=2){
            kv[i/2]= keyValue(kvps[i].toString(), kvps[i+1]);            
        }
        this.logger.info(msg,kv);
    }

    ////////////////////////////////////////////
    ///. ORIGINAL LOGGING
    /// /////////////////////////////////////////
    /// 
    /// 
    ///   //this is an aggregator, it is anticipated that this will get replaced but the api will remain.
    /// 
    /// 
    /// 
    // @Deprecated
    // public void setErrorLevel(LoggingLevel level){
    //     this.errorLevel = level;
    // }
    
    // @Deprecated
    // public void logMessage(Message message){
    //     String line = this.sender + ":" + LocalDate.now() + "\n\t" + message.getMessage() + "\n";
    //     System.out.println(line);
    // }

    // @Deprecated
    // public void logError(Error error){
    //     if (error.getErrorLevel().compareTo(this.errorLevel)>=0){
    //         String line = sender + "issues a " + error.getErrorLevel().toString() + " error:" + LocalDate.now() + "\n\t" + error.getError() + "\n";
    //         System.out.println(line);
    //     }    
    // }

    // @Deprecated
    // public void reportStatus(Status report){
    //     String line = this.sender + ":" + report.getStatus().toString() + ":" + LocalDate.now() + "\n\t" + report.getProgress() + " percent complete." + "\n";
    //     System.out.println(line);
    // }

    ////////////////////////////////////////////
    ///. UTILITY METHODS
    /// /////////////////////////////////////////
    public static Object[] append(Object[] array1, Object[] array2) {
        Object[] result = new Object[array1.length + array2.length];
        System.arraycopy(array1, 0, result, 0, array1.length);
        System.arraycopy(array2, 0, result, array1.length, array2.length);
        return result;
    }
}
