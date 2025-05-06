package usace.cc.plugin.api;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import usace.cc.plugin.api.eventstore.tiledb.TileDbEventStore;


public class DataStoreTypeRegistry{
    public static Map<StoreType, Class<?>> registry;
    static{
        registry = new HashMap<>();
        registry.put(StoreType.S3, FileDataStoreS3.class);
        registry.put(StoreType.TILEDB, TileDbEventStore.class);
        
    }

    @SuppressWarnings("unchecked")
    public static <T> Optional<T> newStore(StoreType st) {
        if (registry.containsKey(st)){
            try{
                var clazz = registry.get(st);
                var instance = clazz.getDeclaredConstructor().newInstance();
                return Optional.of((T)instance);
            } catch (Exception ex){
                //@TODO add logging.  
                //Currently just fall thorugh to the final Optional.empty()
                ex.printStackTrace();
            }
        }
        return Optional.empty();
    }  
}

/* 
public final class DataStoreTypeRegistry {

    private Map<StoreType,Type> registry;
    private static DataStoreTypeRegistry instance = null;

    private DataStoreTypeRegistry(){
        registry = new HashMap<StoreType,Type>();
        registry.put(StoreType.S3, FileDataStoreS3.class);
    }

    public static DataStoreTypeRegistry getInstance(){
        if (instance==null){
            instance = new DataStoreTypeRegistry();
        }
        return instance;
    }

    public static void register(StoreType storeType, Object storeInstance){
        DataStoreTypeRegistry.getInstance().registry.put(storeType, storeInstance.getClass());
    }

    public Object newStore(StoreType s) throws Exception{
        Type type = DataStoreTypeRegistry.getInstance().registry.get(s);
        // var clazz = type.getClass();
        // System.out.println(clazz);
        // var constructors = FileDataStoreS3.class.getConstructors();
        // var cl = constructors.length;
        // System.out.println(cl);
        // var c0 = constructors[0];
        // var ni = c0.newInstance();
        // System.out.println(ni);
        // System.out.println(constructors);
        //var constr = clazz.getDeclaredConstructor();
        //var instancez = constr.newInstance();
        //System.out.println(instancez);
        var instance = type.getClass().getDeclaredConstructor().newInstance();
        return instance;
    }
}
    */
