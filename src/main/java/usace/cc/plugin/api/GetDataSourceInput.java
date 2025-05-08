package usace.cc.plugin.api;

public class GetDataSourceInput {
    
    private DataSourceIOType dataSourceType;
    private String dataSourceName;
    
    public DataSourceIOType getDataSourceIOType(){
        return dataSourceType;
    }
    
    public String getDataSourceName(){
        return dataSourceName;
    }
    
    public GetDataSourceInput(String name, DataSourceIOType type){
        dataSourceType = type;
        dataSourceName = name;
    }
}
