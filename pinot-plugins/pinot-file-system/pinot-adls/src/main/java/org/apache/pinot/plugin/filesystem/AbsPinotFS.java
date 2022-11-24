package org.apache.pinot.plugin.filesystem;

import com.azure.core.http.ProxyOptions;
import com.azure.core.http.netty.NettyAsyncHttpClientBuilder;
import com.azure.core.http.rest.PagedIterable;
import com.azure.identity.ClientSecretCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlockBlobItem;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.Utility;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.DataLakeServiceClientBuilder;
import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.BasePinotFS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Azure Blob Storage based Pinot Filesystem.
 */
public class AbsPinotFS extends BasePinotFS {
  private static final Logger LOGGER = LoggerFactory.getLogger(AbsPinotFS.class);

  private static final String AUTHENTICATION_TYPE = "authenticationType";
  private static final String ACCOUNT_NAME = "accountName";
  private static final String ACCESS_KEY = "accessKey";
  private static final String CONTAINER_NAME = "containerName";

  private static final String ENABLE_CHECKSUM = "enableChecksum";
  private static final String CLIENT_ID = "clientId";
  private static final String CLIENT_SECRET = "clientSecret";
  private static final String TENANT_ID = "tenantId";
  private static final String PROXY_HOST = "proxyHost";
  private static final String PROXY_PORT = "proxyPort";
  private static final String PROXY_USERNAME = "proxyUsername";
  private static final String PROXY_PASSWORD = "proxyPassword";

  private static final String HTTPS_URL_PREFIX = "https://";

  private static final String AZURE_BLOB_DNS_SUFFIX = ".blob.core.windows.net";
  private static final String PATH_ALREADY_EXISTS_ERROR_CODE = "PathAlreadyExists";
  private static final String CONTAINER_NOT_FOUND_ERROR_CODE = "ContainerNotFound";
  private static final String IS_DIRECTORY_KEY = "hdi_isfolder";

  private static final String PATH_DELIMITER = "/";
  private static final int NOT_FOUND_STATUS_CODE = 404;
  private static final int ALREADY_EXISTS_STATUS_CODE = 409;

  // Azure Data Lake Gen2's block size is 4MB
  private static final int BUFFER_SIZE = 4 * 1024 * 1024;

  // If enabled, pinotFS implementation will guarantee that the bits you've read are the same as the ones you wrote.
  // However, there's some overhead in computing hash. (Adds roughly 3 seconds for 1GB file)
  private boolean _enableChecksum;

  private String _containerName;
  private enum AuthenticationType {
    ACCESS_KEY, AZURE_AD, AZURE_AD_WITH_PROXY
  }

  BlobServiceClient _blobServiceClientClient;

  public AbsPinotFS() {
  }

  public AbsPinotFS(BlobServiceClient blobServiceClient) {
    _blobServiceClientClient = blobServiceClient;
  }

  public void init(PinotConfiguration config) {
    _enableChecksum = config.getProperty(ENABLE_CHECKSUM, false);

    // Azure storage account name
    String accountName = config.getProperty(ACCOUNT_NAME);
    String accessKey = config.getProperty(ACCESS_KEY);
    String blobServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_BLOB_DNS_SUFFIX;

    StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accessKey);


    // TODO: consider to add the encryption of the following config
    DefaultAzureCredential defaultCredential = new DefaultAzureCredentialBuilder().build();
    _blobServiceClientClient = new BlobServiceClientBuilder()
        .endpoint(blobServiceEndpointUrl)
        .credential(sharedKeyCredential)
        .buildClient();



//    String authTypeStr = config.getProperty(AUTHENTICATION_TYPE, ADLSGen2PinotFS.AuthenticationType.ACCESS_KEY.name());
//    ADLSGen2PinotFS.AuthenticationType authType = ADLSGen2PinotFS.AuthenticationType.valueOf(authTypeStr.toUpperCase());
//    String accessKey = config.getProperty(ACCESS_KEY);
//    String fileSystemName = config.getProperty(FILE_SYSTEM_NAME);
//    String clientId = config.getProperty(CLIENT_ID);
//    String clientSecret = config.getProperty(CLIENT_SECRET);
//    String tenantId = config.getProperty(TENANT_ID);
//    String proxyHost = config.getProperty(PROXY_HOST);
//    String proxyUsername = config.getProperty(PROXY_USERNAME);
//    String proxyPassword = config.getProperty(PROXY_PASSWORD);
//    String proxyPort = config.getProperty(PROXY_PORT);
//
//    String dfsServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_STO RAGE_DNS_SUFFIX;
//    String blobServiceEndpointUrl = HTTPS_URL_PREFIX + accountName + AZURE_BLOB_DNS_SUFFIX;
//
//    DataLakeServiceClientBuilder dataLakeServiceClientBuilder =
//        new DataLakeServiceClientBuilder().endpoint(dfsServiceEndpointUrl);
//    BlobServiceClientBuilder blobServiceClientBuilder = new BlobServiceClientBuilder().endpoint(blobServiceEndpointUrl);
//
//    switch (authType) {
//      case ACCESS_KEY: {
//        LOGGER.info("Authenticating using the access key to the account.");
//        Preconditions.checkNotNull(accountName, "Account Name cannot be null");
//        Preconditions.checkNotNull(accessKey, "Access Key cannot be null");
//
//        StorageSharedKeyCredential sharedKeyCredential = new StorageSharedKeyCredential(accountName, accessKey);
//        dataLakeServiceClientBuilder.credential(sharedKeyCredential);
//        blobServiceClientBuilder.credential(sharedKeyCredential);
//        break;
//      }
//      case AZURE_AD: {
//        LOGGER.info("Authenticating using Azure Active Directory");
//        Preconditions.checkNotNull(clientId, "Client ID cannot be null");
//        Preconditions.checkNotNull(clientSecret, "ClientSecret cannot be null");
//        Preconditions.checkNotNull(tenantId, "TenantId cannot be null");
//
//        ClientSecretCredential clientSecretCredential =
//            new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId)
//                .build();
//        dataLakeServiceClientBuilder.credential(clientSecretCredential);
//        blobServiceClientBuilder.credential(clientSecretCredential);
//        break;
//      }
//      case AZURE_AD_WITH_PROXY: {
//        LOGGER.info("Authenticating using Azure Active Directory with proxy");
//        Preconditions.checkNotNull(clientId, "Client Id cannot be null");
//        Preconditions.checkNotNull(clientSecret, "ClientSecret cannot be null");
//        Preconditions.checkNotNull(tenantId, "Tenant Id cannot be null");
//        Preconditions.checkNotNull(proxyHost, "Proxy Host cannot be null");
//        Preconditions.checkNotNull(proxyPort, "Proxy Port cannot be null");
//        Preconditions.checkNotNull(proxyUsername, "Proxy Username cannot be null");
//        Preconditions.checkNotNull(proxyPassword, "Proxy Password cannot be null");
//
//        NettyAsyncHttpClientBuilder builder = new NettyAsyncHttpClientBuilder();
//        builder.proxy(
//            new ProxyOptions(ProxyOptions.Type.HTTP, new InetSocketAddress(proxyHost, Integer.parseInt(proxyPort)))
//                .setCredentials(proxyUsername, proxyPassword));
//        ClientSecretCredentialBuilder clientSecretCredentialBuilder =
//            new ClientSecretCredentialBuilder().clientId(clientId).clientSecret(clientSecret).tenantId(tenantId);
//        clientSecretCredentialBuilder.httpClient(builder.build());
//
//        dataLakeServiceClientBuilder.credential(clientSecretCredentialBuilder.build());
//        blobServiceClientBuilder.credential(clientSecretCredentialBuilder.build());
//        break;
//      }
//      default:
//        throw new IllegalStateException("Expecting valid authType. One of (ACCESS_KEY, AZURE_AD, AZURE_AD_WITH_PROXY");
//    }
//
//    _blobServiceClient = blobServiceClientBuilder.buildClient();
//    DataLakeServiceClient serviceClient = dataLakeServiceClientBuilder.buildClient();
//    _fileSystemClient = getOrCreateClientWithFileSystem(serviceClient, fileSystemName);
//
//    LOGGER.info("ADLSGen2PinotFS is initialized (accountName={}, fileSystemName={}, dfsServiceEndpointUrl={}, "
//            + "blobServiceEndpointUrl={}, enableChecksum={})", accountName, fileSystemName, dfsServiceEndpointUrl,
//        blobServiceEndpointUrl, _enableChecksum);
  }


  @Override
  protected boolean doMove(URI srcUri, URI dstUri)
      throws IOException {
    BlobContainerClient srcBlobContainerClient = _blobServiceClientClient.getBlobContainerClient(srcUri.getHost());
    BlobContainerClient dstBlobContainerClient = _blobServiceClientClient.getBlobContainerClient(srcUri.getHost());

    srcBlobContainerClient.getBlobClient().copyFromUrl();
    dstBlobContainerClient.getBlobClient().beginCopy()
    return false;
  }

  @Override
  public boolean mkdir(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public boolean delete(URI segmentUri, boolean forceDelete)
      throws IOException {
    return false;
  }

  @Override
  public boolean copyDir(URI srcUri, URI dstUri)
      throws IOException {
    return false;
  }

  @Override
  public boolean exists(URI fileUri)
      throws IOException {
    return false;
  }

  @Override
  public long length(URI fileUri)
      throws IOException {
    return 0;
  }

  @Override
  public String[] listFiles(URI fileUri, boolean recursive)
      throws IOException {
    BlobContainerClient blobContainerClient = _blobServiceClientClient.getBlobContainerClient(fileUri.getHost());
    String rootPathPrefix = Utility.urlDecode(AzurePinotFSUtil.convertUriToUrlEncodedAzureStylePath(fileUri));
    List<String> paths = new ArrayList<>();
    walk(rootPathPrefix, paths, blobContainerClient, recursive);
    return paths.toArray(new String[0]);
  }

  public void walk(String rootPathPrefix, List<String> paths, BlobContainerClient blobContainerClient,
      boolean recursive) {
    PagedIterable<BlobItem> blobItems = blobContainerClient.listBlobsByHierarchy(rootPathPrefix);
    for (BlobItem blobItem : blobItems) {
      boolean isDirectory = blobItem.isPrefix() != null && blobItem.isPrefix();
      String currentName = blobItem.getName();
      paths.add(AzurePinotFSUtil.convertAzureStylePathToUriStylePath(currentName));
      currentName = isDirectory ? blobItem.getName().substring(0, currentName.length() - 1) : currentName;
      if (isDirectory && recursive) {
        // recursively walk if the current path is directory
        String childRootPath = StringUtils.isEmpty(rootPathPrefix) ? currentName + PATH_DELIMITER
            : rootPathPrefix + PATH_DELIMITER + currentName + PATH_DELIMITER;
        walk(childRootPath, paths, blobContainerClient, true);
      }
    }
  }

  @Override
  public void copyToLocalFile(URI srcUri, File dstFile)
      throws Exception {

  }

  @Override
  public void copyFromLocalFile(File srcFile, URI dstUri)
      throws Exception {

  }

  @Override
  public boolean isDirectory(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public long lastModified(URI uri)
      throws IOException {
    return 0;
  }

  @Override
  public boolean touch(URI uri)
      throws IOException {
    return false;
  }

  @Override
  public InputStream open(URI uri)
      throws IOException {
    return null;
  }
}
