package nifi.azure.dlstore;

import org.apache.nifi.controller.ControllerService;

import com.microsoft.azure.datalake.store.ADLStoreClient;

public interface AzureDLPropertiesService extends ControllerService{
    String getProperty(String key);
	ADLStoreClient getAdlsClient();
}
