package usace.cc.plugin.api;
 
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import usace.cc.plugin.api.Error.ErrorLevel;
import usace.cc.plugin.api.IOManager.InvalidDataStoreException;

public final class PluginManager {

    private CcStore cs;
    private Payload payload;
    private Logger logger;
    private String eventIdentifier;
    private static PluginManager instance = null;
    //private boolean hasUpdatedPaths = false;
    private Pattern p;
    private final String pathPattern = "(?<=\\{).+?(?=\\})";

    
    public static PluginManager getInstance() throws InvalidDataStoreException{
        if (instance==null){
            instance = new PluginManager();
        }
        return instance;
    }
    
    
    private PluginManager() throws InvalidDataStoreException{
        p = Pattern.compile(pathPattern);
        String sender = System.getenv(EnvironmentVariables.CC_PLUGIN_DEFINITION);
        eventIdentifier=System.getenv(EnvironmentVariables.CC_EVENT_IDENTIFIER);
        logger = new Logger(sender, ErrorLevel.WARN);
        cs = new CcStoreS3();
        try {
            this.payload = cs.getPayload();
            this.connectStores(payload.getStores());
            for (Action action : payload.getActions()) {
                this.connectStores(action.getStores());
            }
            substitutePathVariables();
        } catch (Exception e) {
            throw new InvalidDataStoreException(e);
        }
        
    }

    
    public Payload getPayload(){
        return payload;
    }

    public String getEventIdentifier() {
        return this.eventIdentifier;
    }

    public void setLogLevel(ErrorLevel level){
        logger.setErrorLevel(level);
    }

    public void logMessage(Message message){
        logger.logMessage(message);
    }

    public void logError(Error error){
        logger.logError(error);
    }
    
    public void reportProgress(Status report){
        logger.reportStatus(report);
    }


    private void connectStores(DataStore[] stores) throws Exception{
        for (DataStore store : stores){
            Optional<ConnectionDataStore> dsOpt = DataStoreTypeRegistry.newStore(store.getStoreType());
            if (dsOpt.isPresent()){
                var dsInstance = dsOpt.get();
                var conn = dsInstance.connect(store);
                store.setSession(conn);
            }
        }
    }

    private void substitutePathVariables(){
        var attrs = this.payload.getAttributes();

        for (DataSource ds : this.payload.getInputs()){ 
            substitutePaths(ds,attrs);
        }

        for (DataSource ds : this.payload.getOutputs()){ 
            substitutePaths(ds,attrs);
        }

        for (Action action :this.payload.getActions()){
            //substitute map vars
            substituteAttributes(action.getAttributes());
  
            for (DataSource ds : action.getInputs()){ 
                substitutePaths(ds,attrs);
            }
    
            for (DataSource ds : action.getOutputs()){ 
                substitutePaths(ds,attrs);
            }
        }
    }

    private void substituteAttributes(PayloadAttributes pattrs){
        var attrs = pattrs.getAttributes();
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            //var key = entry.getKey();
            var val = entry.getValue();
            if (val instanceof String){
                parameterSubstitute((String)val, pattrs);
            }            
        }
    }

    //a.k.a. pathssubstitute
    private void substitutePaths(DataSource ds, PayloadAttributes attrs){
        var param = parameterSubstitute(ds.getName(),attrs); 
        ds.setName(param);
        
        var pathOpt = ds.getPaths();
        if (pathOpt.isPresent()){
            var paths = pathOpt.get();
            for (String key : paths.keySet()){
                paths.put(key, parameterSubstitute(paths.get(key), attrs));
            }
            ds.setPaths(paths);
        }
       

        var datapathsOpt = ds.getDataPaths();
        if (datapathsOpt.isPresent()){
            var datapaths = datapathsOpt.get();
            for (String key : datapaths.keySet()){
                datapaths.put(key, parameterSubstitute(datapaths.get(key), attrs));
            }
            ds.setDataPaths(datapaths);
        }
    }

    private String parameterSubstitute(String param, PayloadAttributes attrs) {
        Matcher m = p.matcher(param);
        while(m.find()){
            String result = m.group();
            String[] parts = result.split("::", 0);
            String prefix = parts[0];
            String subname = parts[1];
            switch(prefix){
                case "ENV":
                    String val = System.getenv(subname);
                    param = param.replaceFirst("\\{"+result+"\\}", val);//?
                    m = p.matcher(param);
                break;
                case "ATTR":
                    //Optional<String> valattr = attrs.<String>get(subname);
                    //Optional<String> valattr = (Optional<String>)attrs.get(subname);
                    //Optional<String> valattr = attrs.getAlt1(subname, String.class);
                    Optional<String> optVal = attrs.get(subname);
                    if (optVal.isPresent()){
                        param = param.replaceFirst("\\{"+result+"\\}", optVal.get());//?
                        m = p.matcher(param);
                    } else{
                        //@TODO logging is kind of annoying
                        logger.logMessage(new Message(String.format("Attribute %s not found", subname)));
                    }
                break;
            }
        }
        return param;
    }
}