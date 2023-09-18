/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.message.BasicHeader;
import org.junit.After;
import org.junit.Before;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

public class SparseEncodingProcessIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "sparse_encoding_index";

    private static final String PIPELINE_NAME = "pipeline-sparse-encoding";

    @After
    @SneakyThrows
    public void tearDown() {
        super.tearDown();
        /* this is required to minimize chance of model not being deployed due to open memory CB,
         * this happens in case we leave model from previous test case. We use new model for every test, and old model
         * can be undeployed and deleted to free resources after each test case execution.
         */
        findDeployedModels().forEach(this::deleteModel);
    }

    @Before
    public void setPipelineName() {
        this.setPipelineConfigurationName("processor/SparsePipelineConfiguration.json");
    }

    public void testSparseEncodingProcessor() throws Exception {
        String modelId = uploadSparseEncodingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME);
        createTextEmbeddingIndex();
        ingestDocument();
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    private String uploadSparseEncodingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadSparseModelRequestBody.json").toURI()));
        return uploadModel(requestBody);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
                INDEX_NAME,
                Files.readString(Path.of(classLoader.getResource("processor/SparseIndexMappings.json").toURI())),
                PIPELINE_NAME
        );
    }

    private void ingestDocument() throws Exception {
        String ingestDocument = "{\n"
                + "\"passage_text\": \"This is a good day\""
                + "}\n";
        Response response = makeRequest(
                client(),
                "POST",
                INDEX_NAME + "/_doc?refresh",
                null,
                toHttpEntity(ingestDocument),
                ImmutableList.of(new BasicHeader(HttpHeaders.USER_AGENT, "Kibana"))
        );
        Map<String, Object> map = XContentHelper.convertToMap(
                XContentType.JSON.xContent(),
                EntityUtils.toString(response.getEntity()),
                false
        );
        assertEquals("created", map.get("result"));
    }


}
