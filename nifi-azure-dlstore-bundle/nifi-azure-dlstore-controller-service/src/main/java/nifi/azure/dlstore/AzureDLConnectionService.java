package nifi.azure.dlstore;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.oauth2.AzureADAuthenticator;
import com.microsoft.azure.datalake.store.oauth2.AzureADToken;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

@Tags({"credentials", "data lake", "azure"})
@CapabilityDescription("Provides a controller service to Azure DL Store credentials properties.")
public class AzureDLConnectionService extends AbstractControllerService implements AzureDLPropertiesService{
    private static final Logger log = LoggerFactory.getLogger(AzureDLConnectionService.class);
    private static ADLStoreClient _adlsClient;

    private static ScheduledExecutorService _service;

    private static String _accountFQDN;
    private static String _authTokenEndpoint;
    private static String _clientId;
    private static String _clientSecret;

    public static final PropertyDescriptor ACCOUNT_FQDN = new PropertyDescriptor.Builder()
            .name("Azure Data Lake Account FQDN")
            .description("Azure Data Lake Account FQDN")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_TOKEN_ENDPOINT = new PropertyDescriptor.Builder()
            .name("Oauth 2.0 Token Endpoint")
            .description("Oauth 2.0 Token Endpoint")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_ID = new PropertyDescriptor.Builder()
            .name("Client Id")
            .description("Azure Client Id")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLEINT_SECRET_KEY = new PropertyDescriptor.Builder()
            .name("Client Secret Key")
            .description("Azure Client Secret Key")
            .required(true)
            .sensitive(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private static final List<PropertyDescriptor> serviceProperties;

    static{
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(ACCOUNT_FQDN);
        descriptors.add(AUTH_TOKEN_ENDPOINT);
        descriptors.add(CLIENT_ID);
        descriptors.add(CLEINT_SECRET_KEY);
        serviceProperties = Collections.unmodifiableList(descriptors);
    }

    private Properties properties = new Properties();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return serviceProperties;
    }

    @OnEnabled
    public void onConfigured(final ConfigurationContext context) throws InitializationException {
        log.info("Starting AzureDLConnectionService.");

        properties.clear();

        properties.put("ACCOUNT_FQDN", context.getProperty(ACCOUNT_FQDN).getValue());
        properties.put("AUTH_TOKEN_ENDPOINT", context.getProperty(AUTH_TOKEN_ENDPOINT).getValue());
        properties.put("CLIENT_ID", context.getProperty(CLIENT_ID).getValue());
        properties.put("CLEINT_SECRET_KEY", context.getProperty(CLEINT_SECRET_KEY).getValue());

        _accountFQDN = context.getProperty(ACCOUNT_FQDN).getValue();
        _authTokenEndpoint = context.getProperty(AUTH_TOKEN_ENDPOINT).getValue();
        _clientId = context.getProperty(CLIENT_ID).getValue();
        _clientSecret = context.getProperty(CLEINT_SECRET_KEY).getValue();

        _service = Executors.newScheduledThreadPool(3, new ThreadFactory() {
            private final ThreadFactory defaultFactory = Executors.defaultThreadFactory();

            @Override
            public Thread newThread(Runnable r) {
                final Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                t.setName("Refresh Azure DL Store token");
                return t;
            }
        });

        Runnable runnable = new Runnable() {
            public void run() {
                _adlsClient = null;

                try
                {
                    AzureADToken token = AzureADAuthenticator.getTokenUsingClientCreds(_authTokenEndpoint, _clientId, _clientSecret);
                    _adlsClient = ADLStoreClient.createClient(_accountFQDN, token);
                }
                catch (ADLException ex)
                {
                    log.error("Could not start AzureDLConnectionService. " + ex.toString());
                }
                catch (Exception e)
                {
                    log.error("Could not start AzureDLConnectionService. " + e.toString());
                }
            }
        };

        try {
            _service.scheduleAtFixedRate(runnable, 0, 24, TimeUnit.HOURS);
        }
        catch (Exception e)
        {
            log.error("Could not start AzureDLConnectionService. " + e.toString());
        }

        //for(String name : properties.stringPropertyNames()){
        //    log.info("Set " + name + " => " + properties.get(name));
        //}
    }

    /**
     * Shutdown pool, close all open connections.
     */
    @OnDisabled
    public void shutdown() {
        try {
            _service.shutdown();
        } catch (Exception e) {
            log.error("Could not shutdown AzureDLConnectionService. " + e.toString());
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public ADLStoreClient getAdlsClient() {
        return _adlsClient;
    }
}
