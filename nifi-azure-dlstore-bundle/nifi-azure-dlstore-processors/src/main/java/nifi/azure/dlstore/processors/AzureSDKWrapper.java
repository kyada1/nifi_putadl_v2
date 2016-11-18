package nifi.azure.dlstore.processors;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

import java.io.IOException;
import java.io.OutputStream;
import java.io.*;

// Wrap call to the non-thread-safe MS SDK routine
public final class AzureSDKWrapper {

	private static Object lockDLCreateFile = new Object();

	// Create a file in Azure DL Store
	public static void DLCreateFile(ADLStoreClient adlsClient,
                                                     String path,
                                                     String contents,
                                                     boolean force)
                                                     throws IOException, ADLException {
		synchronized (lockDLCreateFile) {

            ByteArrayOutputStream s = new ByteArrayOutputStream();
            PrintStream out = new PrintStream(s);
            out.println(contents);
            out.close();

            if (force)
            {
                OutputStream stream = adlsClient.createFile(path, IfExists.OVERWRITE);
                stream.write(s.toByteArray());
                stream.close();
            }
            else
            {
                OutputStream stream = adlsClient.createFile(path, IfExists.FAIL);
                stream.write(s.toByteArray());
                stream.close();
            }
		}
	}
}
