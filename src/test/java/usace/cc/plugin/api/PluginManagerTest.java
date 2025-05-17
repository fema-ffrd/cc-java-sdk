package usace.cc.plugin.api;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;

import usace.cc.plugin.api.IOManager.InvalidDataStoreException;
import usace.cc.plugin.api.eventstore.EventStore.ArrayAttribute;
import usace.cc.plugin.api.eventstore.EventStore.ArrayDimension;
import usace.cc.plugin.api.eventstore.EventStore.ArrayType;
import usace.cc.plugin.api.eventstore.EventStore.AttrType;
import usace.cc.plugin.api.eventstore.EventStore.CreateArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.DimensionType;
import usace.cc.plugin.api.eventstore.EventStore.GetArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.LayoutOrder;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayBuffer;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayInput;
import usace.cc.plugin.api.eventstore.tiledb.TileDbEventStore;


/*
 * getpayload
 * parameterSubtitute
 * pathSubstitute
 * 
 */

public class PluginManagerTest {

    private PluginManager pm;
    private Payload payload;

    @BeforeEach
    void setUp() throws InvalidDataStoreException {
        pm = PluginManager.getInstance();
        payload = pm.getPayload();            
    }
    
    @Test
    public void testGetEventIdentifier() {
        String expectedResult = "1";
        assertEquals(expectedResult, pm.getEventIdentifier());
    }


    @Test 
    void pluginManagerTest() throws Exception{

        PluginManager pm = PluginManager.getInstance();

        Payload payload = pm.getPayload();

        var payloadAttributes = payload.getAttributes();

        //attributes function like a java map of <String,Object>
        for(Entry<String,Object> entry :payloadAttributes.entrySet()){
            System.out.println(String.format("%s::%s",entry.getKey(),entry.getValue()));
        }

        //to get a single value as a specific type
        Optional<Double> testNumberOpt = payloadAttributes.get("testnumber");
        if (testNumberOpt.isPresent()){
            System.out.println(testNumberOpt.get());
        }

        Optional<List<Integer>> testArrayOpt = payloadAttributes.get("testarray");
        if (testArrayOpt.isPresent()){
            List<Integer> testArrayInt = testArrayOpt.get();
            System.out.println(testArrayInt);
        }

        Optional<String> testStringArrayOpt = payloadAttributes.get("teststringarray");
        if (testStringArrayOpt.isPresent()){
            var testArrayString = testStringArrayOpt.get();
            System.out.println(testArrayString);
        }

        //You shouldn't need to access the underlying map, however it is accessible:
        Map<String,Object> attributeMap = payloadAttributes.getAttributes();
        System.out.println(attributeMap);


        //store functions
        //get all stores
        DataStore[] stores = payload.getStores();
        // for (DataStore store:stores){
        //     System.out.println(store.getName());
        //     if ("EVENT_STORE".equals(store.getName())){
        //         var tdb = new TileDbEventStore();
        //         try{
        //             tdb.connect(store);
        //         } catch(Exception ex){
        //             ex.printStackTrace();
        //         }
        //     }
            
        // }

       
        //get a single store by name
        Optional<DataStore> storeOpt2 = payload.getStore(stores[0].getName());
        if(storeOpt2.isPresent()){
            System.out.println(storeOpt2.get().getName());
        }

        //source functions
        //get inpout data sources
        DataSource[] inputSources =  payload.getInputs();
        for (DataSource source: inputSources){
            System.out.println(source.getName());
        }

        //get a single source by name
        Optional<DataSource> sourceOpt = payload.getInputDataSource(inputSources[0].getName());
        if(sourceOpt.isPresent()){
            System.out.println(sourceOpt.get().getName());
        }

        //Actions
        Action[] actions = payload.getActions();
        for (Action action:actions){
            System.out.println(action.getType());
            System.out.println(action.getDescription());

            //actions have private attributes, stores, inputs, and outputs 
            var params = action.getAttributes();
            Optional<String> actionParamOpt = params.get("seed_store_type");
            if (actionParamOpt.isPresent()){
                System.out.println(actionParamOpt.get());
            }

            //attribute searches will also search into the parent (i.e. payload) attributes
            Optional<String> parentParamOpt = params.get("scenario");
            if (parentParamOpt.isPresent()){
                System.out.println(parentParamOpt.get());
            }

        }
        ///////////////

        //////////metadata
        Optional<TileDbEventStore> tiledbStoreOpt = payload.getStoreSession("EVENT_STORE");
        if (tiledbStoreOpt.isPresent()){
            var store = tiledbStoreOpt.get();
            store.putMetadata("TEST99", 1234.9876);
            Double[] val = store.getMetadata("TEST99", Double[].class);
            System.out.println(val);
            Double val1 = store.getMetadata("TEST99", Double.class);
            System.out.println(val1);

            

            var attrs = new ArrayAttribute[]{
                new ArrayAttribute("a1", AttrType.ATTR_FLOAT32),
                new ArrayAttribute("a2", AttrType.ATTR_INT32),
                new ArrayAttribute("a3", AttrType.ATTR_STRING)
            };

            var dims = new ArrayDimension[]{
                new ArrayDimension("Y", DimensionType.DIMENSION_INT, new long[]{1,10},5)
            };

            ///////////////////////////////////
            CreateArrayInput cainput = new CreateArrayInput();
            cainput.arrayPath="/array1";
            cainput.attributes = attrs;
            cainput.dimensions = dims;
            cainput.cellLayout=LayoutOrder.ROWMAJOR;
            cainput.tileLayout=LayoutOrder.COLMAJOR;
            cainput.arrayType=ArrayType.ARRAY_DENSE;

            try{
                store.createArray(cainput);
            } catch(Exception ex){
                ex.printStackTrace();
            }

            //////////////////////////////
            PutArrayInput putInput = new PutArrayInput();
            putInput.arrayPath="/array1";
            putInput.arrayType=ArrayType.ARRAY_DENSE;
            putInput.putLayout=LayoutOrder.ROWMAJOR;
            putInput.bufferRange = new long[]{3,6};
            var buffer1 = new PutArrayBuffer("a1",new float[]{1.1f, 2.2f, 3.3f, 4.4f});
            var buffer2 = new PutArrayBuffer("a2", new int[]{9,8,7,5});
            var buffer3 = new PutArrayBuffer("a3", "thisisthestring");
            buffer3.offsets = new long[]{0,4,6,9};
            putInput.buffers=new PutArrayBuffer[]{buffer1,buffer2,buffer3};

            try{
                store.putArray(putInput);
            } catch(Exception ex){
                ex.printStackTrace();
            }

            //////////////////////////////
            GetArrayInput getInput = new GetArrayInput();
            getInput.arrayPath="/array1";
            getInput.searchOrder=LayoutOrder.ROWMAJOR;
            getInput.bufferRange = new long[]{1,10};
            getInput.attrs = new String[]{"a1","a2","a3"};

            try{
                var result = store.getArray(getInput);
                System.out.println(result);
            } catch(Exception ex){
                ex.printStackTrace();
            }            

        }


        

    }



    public static void printEnv(){
        var env = System.getenv();
        for (Map.Entry<String, String> entry : env.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();
            System.out.println(String.format("%s:%s",key,value));
        }
    }
}
