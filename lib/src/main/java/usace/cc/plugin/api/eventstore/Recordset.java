package usace.cc.plugin.api.eventstore;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import usace.cc.plugin.api.DataStore;
import usace.cc.plugin.api.IOManager.InvalidDataStoreException;
import usace.cc.plugin.api.PluginManager;
import usace.cc.plugin.api.eventstore.EventStore.ArrayAttribute;
import usace.cc.plugin.api.eventstore.EventStore.ArrayDimension;
import usace.cc.plugin.api.eventstore.EventStore.ArrayType;
import usace.cc.plugin.api.eventstore.EventStore.AttrType;
import usace.cc.plugin.api.eventstore.EventStore.CreateArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.DimensionType;
import usace.cc.plugin.api.eventstore.EventStore.GetArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.LayoutOrder;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayBuffer;

/*
 * Constructor...
 * Create() throws Exception
 * Write(buffer) throws Exception
 * ArrayResult Read(recrange) throws Exception 
 */

public class Recordset<T> {

    //Define annotation that will be used to kark the event store attribute name
    //in the Classes we save/read as a recordset
    //private or public fields can be marked as follows:
    //@EventStoreAttr("my-attr-name")
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.FIELD)
    public @interface EventStoreAttr {
        String value();
    }

    //define an internal data structure used to simplify
    //working with recordset attributes
    private static class ArrayAttrData{
        public String fieldName;
        public String attrName;
        public Class<?> attrType;
        public Object buffer;
        public int size;
        public long[] offsets; 
    }

    //normal class fields
    private final String datapath;
    //private final String storeName;
    private final DataStore store; 
    //private final PluginManager pm;
    private List<ArrayAttrData> attrData;
    private int tileExtent = 256;
    private int bufferSize = 0; 


    //constructor
    // public Recordset(String arrayPath, T[] data) throws EventStoreException{
    //     try{
    //         this.attrData = toArrayAttrData(arrayPath,data);

    //     } catch(Exception ex){
    //         throw new EventStoreException(ex);
    //     }
    // }


    public Recordset(DataStore store,  String datapath) throws EventStoreException{
        try{
            this.datapath=datapath;
            this.store=store;
        } catch(Exception ex){
            throw new EventStoreException(ex);
        } 
    }

    public void Create(T[] data) throws Exception{
        this.attrData = toArrayAttrData(datapath,data);
        var session = this.store.getSession();
        if (session instanceof MultiDimensionalArrayStore arrayStore){
            var cai = createArrayInput();
            arrayStore.createArray(cai);
            var pai = putArrayInput();
            arrayStore.putArray(pai);
        } else {
            throw new EventStoreException("invalid store type");
        }
    }

    public T[] Read(Class<?> clazz, long... recrange) throws Exception{
        if(recrange.length==2){
            var session = this.store.getSession();
            if (session instanceof MultiDimensionalArrayStore arrayStore){
                var input = new GetArrayInput();
                input.arrayPath=this.datapath;
                input.attrs=getAttrNames();
                input.bufferRange=recrange;
                input.searchOrder=LayoutOrder.ROWMAJOR;
                var result = arrayStore.getArray(input);
                return toJavaArray(result, clazz);
            } else {
                throw new EventStoreException("Invalid store type");
            }
        } else {
            throw new EventStoreException("Invalid read range");
        }
    }

    @SuppressWarnings("unchecked")
    private T[] toJavaArray(ArrayResult result, Class<?> clazz) throws Exception{
        var buflen = Array.getLength(result.buffers[0]);
        var arrOut = Array.newInstance(clazz, buflen);
        for(int i=0;i<buflen;i++){
            var t = clazz.getDeclaredConstructor().newInstance();
            for(int j=0;j<attrData.size();j++){
                var field = clazz.getDeclaredField(attrData.get(j).fieldName);
                field.setAccessible(true);
                var val = Array.get(result.buffers[j],i);
                field.set(t,val);
            }
            Array.set(arrOut, i, t);
        }
        return (T[])arrOut;
    }

    private String[] getAttrNames(){
        int attrSize = this.attrData.size();
        String[] names = new String[attrSize];
        for(int i=0;i<attrSize;i++){
            names[i]=this.attrData.get(i).attrName;
        }
        return names;
    }


    private PutArrayInput putArrayInput(){
        var pai = new PutArrayInput();
        pai.arrayPath=this.datapath;
        pai.arrayType=ArrayType.ARRAY_DENSE;
        pai.bufferRange= new long[]{1,this.bufferSize};
        pai.putLayout=LayoutOrder.ROWMAJOR;
        
        var buffers = new PutArrayBuffer[this.attrData.size()];

        for (int i=0;i<this.attrData.size();i++){
            var attr = this.attrData.get(i);
            buffers[i]= new PutArrayBuffer(attr.attrName,attr.buffer);
        }

        pai.buffers=buffers;

        return pai;
    }

    private CreateArrayInput createArrayInput(){

        var cai = new CreateArrayInput();
        cai.arrayPath = this.datapath;
        cai.arrayType=ArrayType.ARRAY_DENSE;
        cai.cellLayout=LayoutOrder.ROWMAJOR;
        cai.tileLayout=LayoutOrder.ROWMAJOR;
        cai.dimensions =new ArrayDimension[]{
            new ArrayDimension("d1", DimensionType.DIMENSION_INT, new long[]{1,bufferSize}, tileExtent)
        };
        
        var attributes = new ArrayAttribute[this.attrData.size()];
        for (int i=0;i<this.attrData.size();i++){
            var attr = attrData.get(i);
            attributes[i]=new ArrayAttribute(attr.attrName, AttrType.fromJavaType(attr.attrType));
        }

        cai.attributes=attributes;
        return cai;
    }

    private List<ArrayAttrData> toArrayAttrData(String arrayPath, T[] data) throws Exception{
        List<ArrayAttrData> arrayAttrs = new ArrayList<>(); 

        //create ArrayAttrData values
        Class<?> clazz = data.getClass();
        if (clazz.isArray()){
            this.bufferSize = Array.getLength(data);
            
            if(this.tileExtent>bufferSize){
                this.tileExtent=bufferSize;
            }
            
            Class<?> componentType = clazz.getComponentType();
            for (Field f : componentType.getDeclaredFields()){
                System.out.println(f.getName());
                if (f.isAnnotationPresent(EventStoreAttr.class)){
                    ArrayAttrData aad = new ArrayAttrData();
                    var annotation = f.getDeclaredAnnotation(EventStoreAttr.class);
                    aad.fieldName=f.getName();
                    aad.attrName = annotation.value();
                    aad.attrType = f.getType();
                    aad.buffer = Array.newInstance(aad.attrType, this.bufferSize);
                    arrayAttrs.add(aad);
                }
            }

            //loop over data and populate ArrayAttrData buffers
            for(int i=0;i<this.bufferSize;i++){
                T rec = data[i];
                for(ArrayAttrData attr: arrayAttrs){
                    var field = rec.getClass().getDeclaredField(attr.fieldName);
                    field.setAccessible(true);
                    var val = field.get(rec);
                    Array.set(attr.buffer, i, val);                
                }
            }
        }
        return arrayAttrs;
    }
    
}
