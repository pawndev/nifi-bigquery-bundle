package org.apache.nifi.processors.bigquery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.*;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.bigquery.utils.NotNullValuesHashMap;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.io.IOException;
import java.util.*;

import static org.apache.nifi.processors.bigquery.PutBigquery.DATASET;
import static org.apache.nifi.processors.bigquery.PutBigquery.TABLE;

@SupportsBatching
@Tags({"Google", "BigQuery", "Google Cloud", "Execute", "Select"})
@CapabilityDescription("SQL request to be executed in  big query")
public class ExecuteBigQuery extends AbstractBigqueryProcessor {

    static final int BUFFER_SIZE = 65536;

    public static final Relationship REL_SUCCESS =
            new Relationship.Builder().name("success")
                    .description("FlowFiles are routed to this relationship after a successful Google BigQuery operation.")
                    .build();
    public static final Relationship REL_FAILURE =
            new Relationship.Builder().name("failure")
                    .description("FlowFiles are routed to this relationship if the Google BigQuery operation fails.")
                    .build();

    static final PropertyDescriptor SQL = new PropertyDescriptor.Builder()
            .name("Bigquery SQL request")
            .description("The SQL request to be executed.")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Set<Relationship> relationships = Collections.unmodifiableSet(
            new HashSet<>(Arrays.asList(REL_SUCCESS, REL_FAILURE)));

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT, PROJECT, DATASET, TABLE));

    public static final PropertyDescriptor READ_TIMEOUT = new PropertyDescriptor.Builder()
            .name(BigQueryAttributes.JOB_READ_TIMEOUT_ATTR)
            .displayName("Read Timeout")
            .description(BigQueryAttributes.JOB_READ_TIMEOUT_DESC)
            .required(true)
            .defaultValue("5 minutes")
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final String sql = context.getProperty(SQL).getValue();


        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> jsonDocument = mapper.readValue(session.read(flowFile), new TypeReference<NotNullValuesHashMap<String, Object>>() {
            });
            // ---
            
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(sql)
                // Use standard SQL syntax for queries.
                // See: https://cloud.google.com/bigquery/sql-reference/
                .setUseLegacySql(false)
                .build();

            // Create a job ID so that we can safely retry.
            JobId jobId = JobId.of(UUID.randomUUID().toString());
            Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            // Wait for the query to complete.
            queryJob = queryJob.waitFor();

            // Check for errors
            if (queryJob == null) {
              throw new RuntimeException("Job no longer exists");
            } else if (queryJob.getStatus().getError() != null) {
              // You can also look at queryJob.getStatus().getExecutionErrors() for all
              // errors, not just the latest one.
              throw new RuntimeException(queryJob.getStatus().getError().toString());
            }

            // Get the results.
            TableResult result = queryJob.getQueryResults();
            Object res = result.getJsonContent();

            // Print all pages of the results.
            //for (FieldValueList row : result.iterateAll()) {
              //String url = row.get("url").getStringValue();
              //long viewCount = row.get("view_count").getLongValue();
              //System.out.printf("url: %s views: %d%n", url, viewCount);
            //}
            // how to convert TableResult to json, to send in flowfile
            //Map<String, String>
            flowFile = session.putAttribute(flowFile, "BIG_QUERY_RESULT", res);
            // ---
            //InsertAllRequest.RowToInsert rowToInsert = InsertAllRequest.RowToInsert.of(jsonDocument);
            //BigQuery bigQuery = getBigQuery();

            //InsertAllRequest insertAllRequest = InsertAllRequest.of(dataset, table, rowToInsert);


            //InsertAllResponse insertAllResponse = bigQuery.insertAll(insertAllRequest);

            if (insertAllResponse.hasErrors()) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }


        } catch (IOException ioe) {
            getLogger().error("IOException while reading JSON item: " + ioe.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }


    }
}
