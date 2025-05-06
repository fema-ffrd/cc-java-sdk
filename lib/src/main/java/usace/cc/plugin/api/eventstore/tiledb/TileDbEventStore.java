package usace.cc.plugin.api.eventstore.tiledb;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import usace.cc.plugin.api.ConnectionDataStore;
import usace.cc.plugin.api.DataStore;
import usace.cc.plugin.api.eventstore.EventStoreException;
import usace.cc.plugin.api.eventstore.MetadataStore;
import usace.cc.plugin.api.utils.ResourceScope;
import usace.cc.plugin.api.eventstore.EventStore.ArrayDimension;
import usace.cc.plugin.api.eventstore.EventStore.ArrayResult;
import usace.cc.plugin.api.eventstore.EventStore.AttrType;
import usace.cc.plugin.api.eventstore.EventStore.CreateArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.GetArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayBuffer;
import usace.cc.plugin.api.eventstore.EventStore.PutArrayInput;
import usace.cc.plugin.api.eventstore.EventStore.ArrayAttribute;
import io.tiledb.java.api.*;


public class TileDbEventStore implements MetadataStore, ConnectionDataStore, AutoCloseable {

    private Context ctx;
	private TileDbConfig config;

	private final String defaultMetadataPath = "/scalars";
	private final String defaultAttributeName = "a";

	private final boolean UNSIGNED = true;

	private static Map<usace.cc.plugin.api.eventstore.EventStore.ArrayType,ArrayType> eventStore2TileDbType;
	private static Map<usace.cc.plugin.api.eventstore.EventStore.LayoutOrder,Layout> eventStore2TileDbLayout;

	static{

		eventStore2TileDbType = new HashMap<>(){{
			put(usace.cc.plugin.api.eventstore.EventStore.ArrayType.ARRAY_DENSE, ArrayType.TILEDB_DENSE);
			put(usace.cc.plugin.api.eventstore.EventStore.ArrayType.ARRAY_SPARSE, ArrayType.TILEDB_SPARSE);
		}};

		eventStore2TileDbLayout = new HashMap<>(){{
			put(usace.cc.plugin.api.eventstore.EventStore.LayoutOrder.ROWMAJOR, Layout.TILEDB_ROW_MAJOR);
			put(usace.cc.plugin.api.eventstore.EventStore.LayoutOrder.COLMAJOR, Layout.TILEDB_COL_MAJOR);
			put(usace.cc.plugin.api.eventstore.EventStore.LayoutOrder.UNORDERED, Layout.TILEDB_UNORDERED);
		}};

	}

	@Override
	public void close() throws Exception {
		this.ctx.close();
	}

    @Override
    public Object rawSession() {
        return this.ctx;
    }
    

    @Override
    public ConnectionDataStore connect(DataStore ds) throws FailedToConnectError, EventStoreException{
        //@TODO make "root" a constant?
        Optional<String> rootPathOpt = ds.getParameters().get("root");
        if (rootPathOpt.isEmpty()){
            throw new EventStoreException("Missing root parameter for TILEDB Store");
        }
		
        try(var config = new TileDbConfig(rootPathOpt.get())){
            this.ctx = new Context(config.getTileDbConfig());
			this.config=config;
        } catch(TileDBError ex){
            throw new FailedToConnectError(ex);
        }

		createMetadataAttributeArray();

        return this;        
    }

    private void createMetadataAttributeArray() throws EventStoreException {
		var uri = this.config.uri+this.defaultMetadataPath;
		try{
			if (!Array.exists(ctx, uri)){
				Dimension<Integer> d1 = new Dimension<>(ctx, "rows", Integer.class, new Pair<Integer, Integer>(1, 1), 1);
				Domain domain = new Domain(ctx);
				domain.addDimension(d1);
				ArraySchema schema = new ArraySchema(ctx, ArrayType.TILEDB_DENSE);
				schema.setDomain(domain);				
				schema.setCellOrder(Layout.TILEDB_ROW_MAJOR);
				schema.setTileOrder(Layout.TILEDB_ROW_MAJOR);
				Attribute a1 = new Attribute(ctx, defaultAttributeName, Integer.class);
				schema.addAttribute(a1);
				Array.create(uri, schema);
			}
		} catch (TileDBError ex){
			throw new EventStoreException(ex);
		}
	}

	public void putMetadata(String key, Object val) throws EventStoreException{
		var uri = this.config.uri+this.defaultMetadataPath;
		putMetadata(uri, key, val);
	} 

    public <T> T getMetadata(String key, Class<?> clazz) throws EventStoreException{
		var uri = this.config.uri+this.defaultMetadataPath;
		return getMetadata(uri, key, clazz);
	}

	public void putMetadata(String uri , String key, Object val) throws EventStoreException{
		try(Array array = new Array(ctx,uri,QueryType.TILEDB_WRITE)){
			var na = toNativeArray(ctx, val);
			array.putMetadata(key, na);	
		} catch(TileDBError ex){
			throw new EventStoreException(ex);
		}
	} 

	@SuppressWarnings("unchecked")
    public <T> T getMetadata(String uri, String key, Class<?> clazz) throws EventStoreException{
		try(Array array = new Array(ctx,uri,QueryType.TILEDB_READ);){
			var nativeArray = array.getMetadata(key);
			var javaArray = nativeArray.toJavaArray();
			if (clazz.isArray()){
				return (T)box(javaArray,clazz);
			} else {
				var val = java.lang.reflect.Array.get(javaArray,0);
				return (T)val;
			}	
			
		} catch(Exception ex){
			throw new EventStoreException(ex);
		}
	}
	

	//@TODO ....
	/*
	 * - need to handle missing arrayType fetch from hashmap
	 *  - need to handle autoclose or add final block for all C tiledb calls!!!!
	 *     - schem, domain, dimension
	 */
	public void createArray(CreateArrayInput input) throws Exception{
		var uri = this.config.uri+input.arrayPath;

		if (Array.exists(ctx, uri)){
			throw new EventStoreException(String.format("Array %s already exists",input.arrayPath));
		}
		
		try(ResourceScope scope = new ResourceScope()){
			ArraySchema schema = scope.add(new ArraySchema(ctx, eventStore2TileDbType.get(input.arrayType)));

			//@TODO...hashmap lookup on nexst two line can return a null...
			schema.setCellOrder(eventStore2TileDbLayout.get(input.cellLayout));
			schema.setTileOrder(eventStore2TileDbLayout.get(input.tileLayout));
			
			Domain domain = scope.add(new Domain(ctx));
			for (ArrayDimension arrayDim: input.dimensions){
				Dimension<?> d;
				switch(arrayDim.dimensionType){
					case DIMENSION_STRING:
						d = scope.add(new Dimension<byte[]>(ctx, arrayDim.name, byte[].class, null, null));
						domain.addDimension(d);
						break;
					case DIMENSION_INT:
						d = scope.add(new Dimension<Long>(
							ctx,
							arrayDim.name,
							Long.class,
							new Pair<Long, Long>(arrayDim.domain[0], arrayDim.domain[1]), // Domain: [1, 100]
							arrayDim.tileExtent  // Tile extent
						));
						domain.addDimension(d);
						break;
					default:
						throw new EventStoreException("Invalid dimension type");
				}
			}
			schema.setDomain(domain);	

			for (ArrayAttribute attribute : input.attributes){
				Attribute a = scope.add(new Attribute(ctx, attribute.name, AttrType.toJavaType(attribute.dataType)));
				if(attribute.dataType==AttrType.ATTR_STRING){
					a.setCellValNum(Constants.TILEDB_VAR_NUM);
				}
				schema.addAttribute(a);
			}

			Array.create(uri, schema);

		} catch(TileDBError ex){
			throw new EventStoreException(ex);
		}
	}

	public void putArray(PutArrayInput input) throws Exception{
		var uri = this.config.uri+input.arrayPath;
		if (Array.exists(ctx, uri)){
			try(ResourceScope scope = new ResourceScope()){	
				Array array = scope.add(new Array(ctx, uri, QueryType.TILEDB_WRITE ));
				Query query = scope.add(new Query(array));
				if(input.arrayType==usace.cc.plugin.api.eventstore.EventStore.ArrayType.ARRAY_DENSE){
					SubArray subarray = scope.add(new SubArray(ctx,array));
					query.setLayout(eventStore2TileDbLayout.get(input.putLayout));
					subarray.setSubarray(toNativeArray(ctx, input.bufferRange));
					query.setSubarray(subarray);
				} else {
					query.setLayout(Layout.TILEDB_UNORDERED);
				}
				
				for (PutArrayBuffer buffer : input.buffers){
					query.setDataBuffer(buffer.attrName,toNativeArray(ctx, buffer.buffer));
					if(buffer.offsets!=null){
						query.setOffsetsBuffer(buffer.attrName, toNativeArray(ctx, buffer.offsets, UNSIGNED));
					}
				}
				
				query.submit();
			} 
		} else {
			throw new EventStoreException(String.format("Array does not exist: %s",input.arrayPath));
		}
	}

	public ArrayResult getArray(GetArrayInput input) throws Exception{
		var uri = this.config.uri+input.arrayPath;
		if (Array.exists(ctx, uri)){
			try(ResourceScope scope = new ResourceScope()){	
				Array array = scope.add(new Array(ctx, uri, QueryType.TILEDB_READ));
				Query query = scope.add(new Query(array));
				SubArray subarray = scope.add(new SubArray(ctx,array));
				ArraySchema schema = array.getSchema();

				subarray.setSubarray(toNativeArray(ctx, input.bufferRange));
				query.setSubarray(subarray);
				query.setLayout(eventStore2TileDbLayout.get(input.searchOrder)); //@TODO Fix me!!!!
				HashMap<String, Pair<Long, Long>> max_sizes = query.getResultEstimations();
				for (String attr : input.attrs){
					Pair<Long,Long> maxSize = max_sizes.get(attr);
					var attrType = schema.getAttribute(attr);
					query.setDataBuffer(attr, new NativeArray(ctx, max_sizes.get(attr).getSecond().intValue(), attrType.getType().javaClass()));
					//check for variable length data
					var first = maxSize.getFirst();
					if (first!=null){
						query.setOffsetsBuffer(attr, new NativeArray(ctx, first.intValue(), Datatype.TILEDB_UINT64)); //offsets  //@TODO..should these intValues be longValues?
					}
				}

				query.submit();
				return toArrayResult(input, query);
			}
		} else {
			throw new EventStoreException(String.format("Array does not exist: %s",input.arrayPath));
		}

	}

	public ArrayResult toArrayResult(GetArrayInput input, Query query) throws TileDBError{
		ArrayResult result = new ArrayResult();
		Object[] buffers = new Object[input.attrs.length];
		for (int i=0;i<input.attrs.length;i++){
			buffers[i]=query.getBuffer(input.attrs[i]);
		}
		result.buffers=buffers;
		return result;
	}


 private NativeArray toNativeArray(Context ctx, Object val, boolean unsigned) throws TileDBError, EventStoreException{
	var clazz = val.getClass();

	//var int8Type = Datatype.TILEDB_INT8;
	//var int16Type = Datatype.TILEDB_INT16;
	var int32Type = Datatype.TILEDB_INT32;
	var int64Type = Datatype.TILEDB_INT64;

	if (unsigned){
		//int8Type=Datatype.TILEDB_UINT8;
		//int16Type=Datatype.TILEDB_UINT16;
		int32Type=Datatype.TILEDB_UINT32;
		int64Type=Datatype.TILEDB_UINT64;
	}

	//handle float64
	if (clazz==Double.class || clazz==double.class){
		double[] naVal = {((Number)val).doubleValue()};
		return new NativeArray(ctx, naVal, Datatype.TILEDB_FLOAT64);
	} else if (clazz==Double[].class || clazz==double[].class){
		return new NativeArray(ctx, val, Datatype.TILEDB_FLOAT64);
	}

	//handle float32
	else if (clazz==Float.class || clazz==float.class){
		float[] naVal = {((Number)val).floatValue()};
		return new NativeArray(ctx, naVal, Datatype.TILEDB_FLOAT32);
	} else if (clazz==Float[].class || clazz==float[].class){
		return new NativeArray(ctx, val, Datatype.TILEDB_FLOAT32);
	}

	//handle int64
	else if (clazz==Long.class || clazz==long.class){
		long[] naVal = {((Number)val).longValue()};
		return new NativeArray(ctx, naVal, int64Type);
	} else if (clazz==Long[].class || clazz==long[].class){
		return new NativeArray(ctx, val, int64Type);
	}

	//handle int32
	else if (clazz==Integer.class || clazz==int.class){
		int[] naVal = {((Number)val).intValue()};
		return new NativeArray(ctx, naVal, int32Type);
	} else if (clazz==Integer[].class || clazz==int[].class){
		return new NativeArray(ctx, val, int32Type);
	}

	//handle strings...only utf8 
	else if (clazz==String.class){
		return new NativeArray(ctx, val, Datatype.TILEDB_STRING_ASCII);
	}

	//handle byte arrays
	else if (clazz==byte[].class){
		return new NativeArray(ctx, val, Datatype.TILEDB_UINT8);
	}

	throw new EventStoreException("Invalid data type");
}



	private NativeArray toNativeArray(Context ctx, Object val) throws TileDBError, EventStoreException{
		return toNativeArray(ctx, val,false);
	}

	/**
	 * Converts a primitive array (e.g., {@code int[]}, {@code double[]}) or an Object array into
	 * a boxed array of the specified component type.
	 * <p>
	 * This method performs unchecked casts and assumes that the input array elements
	 * are compatible with the target boxed type. It is intended for internal use where type safety
	 * is managed by the caller.
	 * 
	 * @param <T> the type of elements in the returned array
	 * @param arr the input array (must be non-null and an array type)
	 * @param returnType the expected component type of the returned array; if this is itself an array type, its component type is used
	 * @return a new array of type {@code T[]} containing the boxed elements from {@code arr}
	 * @throws IllegalArgumentException if {@code arr} is not an array
	 * @throws ArrayStoreException if elements cannot be cast to {@code T}
	 */
	@SuppressWarnings("unchecked")
	private static <T> T[] box(Object arr, Class<?> returnType) {
		if (returnType.isArray()){
			returnType = returnType.componentType();
		}
		var arrlen = java.lang.reflect.Array.getLength(arr);
		T[] boxed = (T[])java.lang.reflect.Array.newInstance(returnType,arrlen);
		for (int i = 0; i < arrlen; i++) {
			boxed[i] = (T)returnType.cast(java.lang.reflect.Array.get(arr, i)); // unsafe autoboxing..I don't care.  I'm not writing a version for every type.
		}
		return boxed;
	}









    
}




