package nifi.azure.dlstore.processors;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

import java.io.IOException;
import java.io.OutputStream;

// Wrap call to the non-thread-safe MS SDK routine
public final class AzureSDKWrapper {

	private static Object lockGetToken = new Object();
	private static Object lockGetDLClient = new Object();
	private static Object lockDLCreateFile = new Object();

	// Token based credentials for use with a REST Service Client
	public static AzureADToken GetToken(String authTokenEndpoint, String clientId, String clientSecret)
                                throws IOException{
		synchronized (lockGetToken) {
            AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(authTokenEndpoint, clientId, clientSecret);
			return token;
		}
	}

	// Creates Data Lake Store account management client
	public static ADLStoreClient GetDLClient(String accountFQDN, AzureADToken token) {
		synchronized (lockGetDLClient) {
            ADLStoreClient adlsClient = ADLStoreClient.createClient(accountFQDN, token);
			return adlsClient;
		}
	}


	// Create a file in Azure DL Store
	public static void DLCreateFile(ADLStoreClient adlsClient,
                                                     String path,
                                                     String contents,
                                                     boolean force)
                                                     throws IOException, ADLException {
		synchronized (lockDLCreateFile) {

            byte[] bytesContents = contents.getBytes();

            if (force)
            {
                OutputStream stream = adlsClient.createFile(path, IfExists.OVERWRITE);
                stream.write(bytesContents);
                stream.close();
            }
            else
            {
                OutputStream stream = adlsClient.createFile(path, IfExists.FAIL);
                stream.write(bytesContents);
                stream.close();
            }
		}
	}
}
