package usace.cc.plugin.api.eventstore;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import usace.cc.plugin.api.eventstore.EventStore.AttrType;
import usace.cc.plugin.api.eventstore.EventStoreException;

public interface EventStore {

    public static enum ArrayType{
        ARRAY_DENSE(0),
        ARRAY_SPARSE(1);

        private final int arrayType;

        ArrayType(int arraytype) {
            this.arrayType = arraytype;
        }

        public int getArrayType() {
            return arrayType;
        }

        public static ArrayType fromCode(int arraytype) {
            for (ArrayType arrayType : ArrayType.values()) {
                if (arrayType.getArrayType() == arraytype) {
                return arrayType;
                }
            }
            throw new IllegalArgumentException("Invalid Array Type: " + arraytype);
        }
    }

    public static enum DimensionType{
        DIMENSION_INT(0),
        DIMENSION_STRING(1);

        private final int dimensionType;

        DimensionType(int dimensionType) {
            this.dimensionType = dimensionType;
        }

        public int getDimensionType() {
            return dimensionType;
        }

        public static DimensionType fromCode(int dimensiontype) {
            for (DimensionType dimensionType : DimensionType.values()) {
                if (dimensionType.getDimensionType() == dimensiontype) {
                return dimensionType;
                }
            }
            throw new IllegalArgumentException("Invalid Dimension Type: " + dimensiontype);
        }
    }

    public static enum LayoutOrder{
        ROWMAJOR(0),
        COLMAJOR(1),
        UNORDERED(2);

        private final int order;

        LayoutOrder(int order) {
            this.order = order;
        }

        public int getOrder() {
            return order;
        }

        public static LayoutOrder fromCode(int order) {
            for (LayoutOrder layoutOrder : LayoutOrder.values()) {
                if (layoutOrder.getOrder() == order) {
                return layoutOrder;
                }
            }
            throw new IllegalArgumentException("Invalid layout order: " + order);
        }
    }

    public static enum AttrType{
        ATTR_INT64(0),
        ATTR_INT32(1),
        ATTR_INT16(2),
        ATTR_INT8(3), //currently not suupported
        ATTR_UINT8(4), //user is currently responsible for ensuring this holds unsigned values  use "bydata & 0xff" to convert
        ATTR_FLOAT32(5),
        ATTR_FLOAT64(6),
        ATTR_STRING(7);

        private final int attrtype;

        private final static Map<Class<?>,AttrType> java2TiledbMap = new HashMap<>() {{
            put(Float.class, ATTR_FLOAT32);
            put(float.class, ATTR_FLOAT32);
            put(Double.class, ATTR_FLOAT64);
            put(double.class, ATTR_FLOAT64);
            put(Integer.class, ATTR_INT32);
            put(int.class, ATTR_INT32);
            put(Long.class, ATTR_INT64);
            put(long.class, ATTR_INT64);
            put(Short.class, ATTR_INT16);
            put(Byte.class, ATTR_UINT8);
            put(String.class, ATTR_STRING);
        }};

        AttrType(int attrtype) {
            this.attrtype = attrtype;
        }

        public int getAttrType() {
            return attrtype;
        }

        public static AttrType fromCode(int attrtype) {
            for (AttrType attrType : AttrType.values()) {
                if (attrType.getAttrType() == attrtype) {
                    return attrType;
                }
            }
            throw new EventStoreException("Invalid layout order: " + attrtype);
        }

        public static AttrType fromJavaType(Class<?> clazz) throws EventStoreException{
            if(java2TiledbMap.containsKey(clazz)){
                return java2TiledbMap.get(clazz);
            }
            throw new EventStoreException(String.format("Invalid type for the event store: %s",clazz.getName()));
        }

        public static Class<?> toJavaType(AttrType t) throws EventStoreException{
            for (Entry<Class<?>,AttrType> entry : java2TiledbMap.entrySet()){
                if(entry.getValue()==t){
                    return entry.getKey();
                }
            }
            throw new EventStoreException(String.format("Invalid type for the event store: %s",t));
        }
    }

    
    
    public static class CreateArrayInput{
        public ArrayAttribute[] attributes;
        public ArrayDimension[] dimensions;
        public String arrayPath;
        public ArrayType arrayType;
        public LayoutOrder cellLayout;
        public LayoutOrder tileLayout;
    }

    public static class ArrayAttribute{
        
        public ArrayAttribute(String name, AttrType dataType){
            this.name=name;
            this.dataType=dataType;
        }

        public String name;
        public AttrType dataType;
    }

    public static class ArrayDimension{
        
        public ArrayDimension(String name, DimensionType dtype, long[] domain, long tileExtent){
            this.name=name;
            this.dimensionType=dtype;
            this.domain=domain;
            this.tileExtent=tileExtent;
        }

        public String name;
        public DimensionType dimensionType;
        public long[] domain;
        public long tileExtent;
    }


    public static class PutArrayInput{
        public PutArrayBuffer[] buffers;
        public long[] bufferRange;
        public String arrayPath;
        public ArrayType arrayType; //just used to decide if we will use a dense or sparse array write process.  Probalby could just query the array
        public LayoutOrder putLayout;
    }

    public static class PutArrayBuffer{
        
        public PutArrayBuffer(String attributeName, Object buffer){
            this.attrName=attributeName;
            this.buffer=buffer;
        }

        public PutArrayBuffer(String attributeName, Object buffer, long[] offsets){
            this.attrName=attributeName;
            this.buffer=buffer;
            this.offsets=offsets;
        }

        public String attrName;
        public Object buffer;
        public long[] offsets;   
    }


    public static class GetArrayInput{
        public String[] attrs;
        public String arrayPath;
        public long[] bufferRange;
        public LayoutOrder searchOrder;
    }

}

