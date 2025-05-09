package usace.cc.plugin.api.eventstore.tiledb;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.tiledb.java.api.Config;
import io.tiledb.java.api.TileDBError;
import usace.cc.plugin.api.EnvironmentVariables;
import usace.cc.plugin.api.eventstore.EventStoreException;

public class TileDbConfig implements AutoCloseable{

    public String s3Id;
    public String s3Key;
    public String s3Region;
    public String s3Bucket;
    public String s3Endpoint;
    public String uri;  

    private Config config;

    private final String defaultEventStoreName = "eventdb";

    public TileDbConfig(String rootPath, Optional<String> eventStoreNameOpt){
        
        s3Id = System.getenv(EnvironmentVariables.CC_PROFILE + "_" + EnvironmentVariables.AWS_ACCESS_KEY_ID);
        s3Key = System.getenv(EnvironmentVariables.CC_PROFILE + "_" + EnvironmentVariables.AWS_SECRET_ACCESS_KEY);
        s3Region = System.getenv(EnvironmentVariables.CC_PROFILE + "_" + EnvironmentVariables.AWS_DEFAULT_REGION);
        s3Bucket = System.getenv(EnvironmentVariables.CC_PROFILE + "_" + EnvironmentVariables.AWS_S3_BUCKET);
        s3Endpoint = System.getenv(EnvironmentVariables.CC_PROFILE + "_" +EnvironmentVariables.AWS_ENDPOINT);

        String eventStoreName;
        if(eventStoreNameOpt.isPresent()){
            eventStoreName=eventStoreNameOpt.get();
        } else {
            eventStoreName=defaultEventStoreName;
        }
        uri = String.format("s3://%s/%s/%s", s3Bucket, rootPath, eventStoreName);        
    }

    //user has to handle closing the tiledb config object.
    //var webProtocolRegex *regexp.Regexp = regexp.MustCompile(`^(https?):\/\/(.*)$`)
    public Config getTileDbConfig() throws EventStoreException{
        try {
            config = new Config();
            config.set("vfs.s3.region", s3Region);
            config.set("vfs.s3.aws_access_key_id", s3Id);
            config.set("vfs.s3.aws_secret_access_key", s3Key);
            config.set("vfs.s3.multipart_part_size", String.valueOf(5*1024*1024));
            config.set("vfs.s3.max_parallel_ops", "2");
            if (!( s3Endpoint==null || s3Endpoint.equals(""))) {
                Pattern pattern = Pattern.compile("^(https?)://(.+)$", Pattern.CASE_INSENSITIVE);
                Matcher matcher = pattern.matcher(s3Endpoint);

                if (matcher.find()) {
                    var protocol = matcher.group(1).toLowerCase();
                    var hostAndPath = matcher.group(2).toLowerCase();
                    config.set("vfs.s3.scheme", protocol);
                    config.set("vfs.s3.endpoint_override", hostAndPath);
                    config.set("vfs.s3.use_virtual_addressing", "false");
                } else {
                    throw new EventStoreException("Invalid S3Endpoint.  Endpoint must begin with the protocol: 'http://' or 'https://'.");
                }
            }
            return config;
        } catch (TileDBError e) {
            throw new EventStoreException(e);
        }
    }

    @Override
    public void close() {
        try{
            this.config.close();
        } catch(Exception ex){
            ex.printStackTrace();
        }
    }

}
