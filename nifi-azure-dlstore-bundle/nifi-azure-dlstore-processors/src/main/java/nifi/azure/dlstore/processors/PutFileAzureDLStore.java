/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package nifi.azure.dlstore.processors;

import com.microsoft.azure.datalake.store.ADLException;
import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;

import nifi.azure.dlstore.AzureDLPropertiesService;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.OutputStream;

@Tags({"azure, data lake, store, hdfs, webhdfs"})
@CapabilityDescription("Write FlowFile data to Azure DL Store")
@SeeAlso({})
@WritesAttribute(attribute = "filename", description = "The name of the file written to Azure Data Lake Store comes from the value of this attribute.")
public class PutFileAzureDLStore extends AbstractProcessor {

    private static ADLStoreClient _adlsClient;

    private static String _accountFQDN;
    private static String _authTokenEndpoint;
    private static String _clientId;
    private static String _clientSecret;
    private static String _path;
    private static String _filename;
    private static String _conflictResponse;

    public static final String REPLACE_TRUE_RESOLUTION = "true";
    public static final String REPLACE_FALSE_RESOLUTION = "false";

    public static final Relationship SUCCESS_RELATIONSHIP = new Relationship.Builder()
            .name("File is created")
            .description("File was successfully created in Azure DL Store.")
            .build();

    public static final Relationship FAIL_RELATIONSHIP = new Relationship.Builder()
            .name("File is not created")
            .description("File was NOT successfully created in Azure DL Store.")
            .build();

    // properties
    public static final PropertyDescriptor PROPERTIES_SERVICE = new PropertyDescriptor.Builder()
            .name("Properties Service")
            .description("System properties loader")
            .required(true)
            .identifiesControllerService(AzureDLPropertiesService.class)
            .build();

    public static final PropertyDescriptor DIRECTORY = new PropertyDescriptor.Builder()
            .name("Directory")
            .description("Adzure DL Store Folder Path (for example /aaa/bbb/)")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor FILENAME = new PropertyDescriptor.Builder()
            .name("Filename")
            .required(true)
            .description("The filename to assign to the file when created (for example ${now():format('yyyy-MM-dd-HH-mm-ss')}.txt)")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .expressionLanguageSupported(true)
            .build();

    public static final PropertyDescriptor CONFLICT_RESOLUTION = new PropertyDescriptor.Builder()
            .name("Conflict Resolution Strategy")
            .description("Indicates what should happen when a file with the same name already exists in the output directory.  True - replace/overwrite the file.  False - fail the PutFileAzureDLStore.")
            .required(true)
            .defaultValue(REPLACE_TRUE_RESOLUTION)
            .allowableValues(REPLACE_TRUE_RESOLUTION, REPLACE_FALSE_RESOLUTION)
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    public Set<Relationship> getRelationships(){
        return relationships;
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors(){
        return descriptors;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(PROPERTIES_SERVICE);
        descriptors.add(DIRECTORY);
        descriptors.add(FILENAME);
        descriptors.add(CONFLICT_RESOLUTION);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(FAIL_RELATIONSHIP);
        relationships.add(SUCCESS_RELATIONSHIP);

        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .name(propertyDescriptorName)
                .expressionLanguageSupported(true)
                .dynamic(true)
                .build();
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        final AzureDLPropertiesService adlService = context.getProperty(PROPERTIES_SERVICE).asControllerService(AzureDLPropertiesService.class);
        final java.util.concurrent.atomic.AtomicReference<String> value = new  java.util.concurrent.atomic.AtomicReference<>();

        _path = context.getProperty(DIRECTORY).evaluateAttributeExpressions(flowFile).getValue();
        _filename = context.getProperty(FILENAME).evaluateAttributeExpressions(flowFile).getValue();
        _conflictResponse = context.getProperty(CONFLICT_RESOLUTION).getValue();

        _accountFQDN = adlService.getProperty("ACCOUNT_FQDN");
        _authTokenEndpoint = adlService.getProperty("AUTH_TOKEN_ENDPOINT");
        _clientId = adlService.getProperty("CLIENT_ID");
        _clientSecret = adlService.getProperty("CLEINT_SECRET_KEY");

        try
        {
            _adlsClient = adlService.getAdlsClient();
            AzureSDKWrapper AzureSDKWrapperObject = new AzureSDKWrapper();

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    String contents = org.apache.commons.io.IOUtils.toString(in);
                    value.set(contents);
                }
            });

            // If the replace file is true
            if (_conflictResponse.equals(REPLACE_TRUE_RESOLUTION))
            {
                AzureSDKWrapperObject.DLCreateFile(_adlsClient, _path + _filename, value.get(), true);
                //CreateFile(_path + _filename, value.get(), true);
            }
            else
            {
                AzureSDKWrapperObject.DLCreateFile(_adlsClient, _path + _filename, value.get(), false);
                //CreateFile(_path + _filename, value.get(), false);
            }

            getLogger().info("File was successfully created.");
            session.transfer(flowFile, SUCCESS_RELATIONSHIP);
        }
        catch (ADLException e)
        {
            getLogger().error("File was not created: " + _path + _filename + " " + e.toString());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAIL_RELATIONSHIP);

        } catch (IOException e)
        {
            getLogger().error("Could not read the flowfile content: " + _path + _filename + " " + e.toString());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAIL_RELATIONSHIP);
        }
        catch (Exception e)
        {
            getLogger().error("File was not created: " + _path + _filename + " " + e.toString());
            flowFile = session.penalize(flowFile);
            session.transfer(flowFile, FAIL_RELATIONSHIP);
        }
    }

    // Create file with contents
    public static void CreateFile(String path, String contents, boolean force) throws IOException, ADLException {
        byte[] bytesContents = contents.getBytes();

        if (force)
        {
            OutputStream stream = _adlsClient.createFile(path, IfExists.OVERWRITE);
            stream.write(bytesContents);
            stream.close();
        }
        else
        {
            OutputStream stream = _adlsClient.createFile(path, IfExists.FAIL);
            stream.write(bytesContents);
            stream.close();
        }
    }
}