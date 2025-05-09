package usace.cc.plugin.api.cloud.aws;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.rmi.RemoteException;
import java.util.Optional;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.SdkBaseException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;

import usace.cc.plugin.api.ConnectionDataStore;
import usace.cc.plugin.api.DataStore;
import usace.cc.plugin.api.EnvironmentVariables;
import usace.cc.plugin.api.FileStore;
import usace.cc.plugin.api.StoreType;

//@TODO move all package private vars to class private vars
public class FileStoreS3 implements FileStore, ConnectionDataStore {
    String bucket;
    String postFix;
    StoreType storeType;
    AmazonS3 awsS3;
    AWSConfig config;
    private static String S3ROOT = "root";

    public FileStoreS3(){}

    @Override
    public Boolean copy(FileStore destStore, String srcPath, String destPath) {
        byte[] data;
        try {
            data = getObject(srcPath);
            ByteArrayInputStream bias = new ByteArrayInputStream(data);
            return destStore.put(bias, destPath);
        } catch (RemoteException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public InputStream get(String path) {
        S3Object fullObject = null;
        String key = postFix + "/" + path;
        System.out.println(path);
        System.out.println(bucket);
        try {
            fullObject = awsS3.getObject(new GetObjectRequest(bucket, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            return fullObject.getObjectContent();
        }  catch (Exception e) {
            e.printStackTrace();
            return null;
        } 
    }

    @Override
    public Boolean put(InputStream data, String path) {
        byte[] bytes;
        try {
            bytes = data.readAllBytes();
            return uploadToS3(config.aws_bucket, postFix + "/" + path, bytes);
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
        
    }

    @Override
    public Boolean delete(String path) {
        DeleteObjectRequest dor = new DeleteObjectRequest(config.aws_bucket,postFix + "/" + path);
        try{
            awsS3.deleteObject(dor);
            return true;
        }catch (AmazonServiceException e)
        {
            e.printStackTrace();
            return false;
        }catch(SdkClientException e){
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public Object rawSession(){
        return awsS3;
    }
    
    @Override
    public ConnectionDataStore connect(DataStore ds) throws FailedToConnectError{
        config = new AWSConfig();
        config.aws_access_key_id = System.getenv(ds.getDsProfile() + "_" + EnvironmentVariables.AWS_ACCESS_KEY_ID);
        config.aws_secret_access_key_id = System.getenv(ds.getDsProfile() + "_" + EnvironmentVariables.AWS_SECRET_ACCESS_KEY);
        config.aws_region = System.getenv(ds.getDsProfile() + "_" + EnvironmentVariables.AWS_DEFAULT_REGION);
        config.aws_bucket = System.getenv(ds.getDsProfile() + "_" + EnvironmentVariables.AWS_S3_BUCKET);
        config.aws_endpoint = System.getenv(ds.getDsProfile() + "_"+ EnvironmentVariables.AWS_ENDPOINT);
        //config.aws_disable_ssl = Boolean.parseBoolean(System.getenv(ds.getDsProfile() + "_"+ EnvironmentVariables.S3_DISABLE_SSL));//convert to bool?
        //config.aws_force_path_style = Boolean.parseBoolean(System.getenv(ds.getDsProfile() + "_"+ EnvironmentVariables.S3_FORCE_PATH_STYLE));//convert to bool
        
        Region clientRegion = RegionUtils.getRegion(config.aws_region);//.toUpperCase().replace("-", "_"));//Regions.valueOf(config.aws_region.toUpperCase().replace("-", "_"));
        try {
            
            AWSCredentials credentials = new BasicAWSCredentials(config.aws_access_key_id, config.aws_secret_access_key_id);

            var clientBuilder = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentials));

            if (!(config.aws_endpoint==null || "".equals(config.aws_endpoint))){
                ClientConfiguration clientConfiguration = new ClientConfiguration();
                clientConfiguration.setSignerOverride("AWSS3V4SignerType");
                clientConfiguration.setProtocol(Protocol.HTTP);

                clientBuilder.withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(config.aws_endpoint, clientRegion.getName()))
                    .withPathStyleAccessEnabled(true)
                    .withClientConfiguration(clientConfiguration);

            } else {
                clientBuilder.withRegion(clientRegion.getName());
            }

            awsS3 = clientBuilder.build();

        } catch (SdkClientException e ) {
            //@TODO do we want to print the stackstrace?
            //  I'm converting to a RuntimeException and throwing it up the stack
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        storeType = StoreType.S3;

        String tmpRoot="";
        
        try {
            Optional<String> optParam = ds.getParameters().get(FileStoreS3.S3ROOT);
            if (optParam.isPresent()){
                tmpRoot = optParam.get();
            }
        } catch (Exception e) {
            e.printStackTrace();
            //@TODO  UGH....NOT HANDLING THE ERRORS!!!!!!!!!!!!!!!!
        }
        if (tmpRoot == ""){
            //error out?
            System.out.print("Missing S3 Root Paramter. Cannot create the store.");
        }
        this.bucket = config.aws_bucket;
        tmpRoot = tmpRoot.replaceFirst("^/+", "");
        this.postFix = tmpRoot;
        return this;
    }

    private byte[] getObject(String path) throws RemoteException {
        byte[] data;
        try {
            data = downloadBytesFromS3(path);
        } catch (Exception e) {
            throw new RemoteException(e.toString());
        }
        return data;
    }

    private byte[] downloadBytesFromS3(String key) throws Exception{
        S3Object fullObject = null;
        key = postFix + "/" + key;
        System.out.println(key);
        System.out.println(bucket);
        try {
            fullObject = awsS3.getObject(new GetObjectRequest(bucket, key));
            System.out.println("Content-Type: " + fullObject.getObjectMetadata().getContentType());
            return fullObject.getObjectContent().readAllBytes();
        }  catch (Exception e) {
            throw e;
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (fullObject != null) {
                try {
                    fullObject.close();
                }  catch (Exception e) {
                    throw e;
                }
            }
        }
    }

    private boolean uploadToS3(String bucketName, String objectKey, byte[] fileBytes) {
        try {
            InputStream stream = new ByteArrayInputStream(fileBytes);
            ObjectMetadata meta = new ObjectMetadata();
            meta.setContentLength(fileBytes.length);
            PutObjectRequest putOb = new PutObjectRequest(bucketName, objectKey,stream, meta);
            PutObjectResult response = awsS3.putObject(putOb);
            System.out.println(response.getETag());
            return true;
        } catch (SdkBaseException e) {
            System.out.println(e.getMessage());
            return false;
        }
    }

}
