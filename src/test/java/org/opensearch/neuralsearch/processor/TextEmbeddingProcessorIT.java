/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import lombok.SneakyThrows;

import org.apache.http.HttpHeaders;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;
import org.junit.After;
import org.opensearch.client.Response;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.neuralsearch.common.BaseNeuralSearchIT;

import com.google.common.collect.ImmutableList;

public class TextEmbeddingProcessorIT extends BaseNeuralSearchIT {

    private static final String INDEX_NAME = "text_embedding_index";

    private static final String PIPELINE_NAME = "pipeline-hybrid";

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

    public void testTextEmbeddingProcessor() throws Exception {
        String modelId = uploadTextEmbeddingModel();
        loadModel(modelId);
        createPipelineProcessor(modelId, PIPELINE_NAME);
        createTextEmbeddingIndex();
        ingestDocument();
        assertEquals(1, getDocCount(INDEX_NAME));
    }

    private String uploadTextEmbeddingModel() throws Exception {
        String requestBody = Files.readString(Path.of(classLoader.getResource("processor/UploadModelRequestBody.json").toURI()));
        return uploadModel(requestBody);
    }

    private void createTextEmbeddingIndex() throws Exception {
        createIndexWithConfiguration(
            INDEX_NAME,
            Files.readString(Path.of(classLoader.getResource("processor/IndexMappings.json").toURI())),
            PIPELINE_NAME
        );
    }

    private void ingestDocument() throws Exception {
        String ingestDocument = "{\n"
            + "  \"title\": \"This is a good day\",\n"
            + "  \"description\": \"daily logging\",\n"
            + "  \"favor_list\": [\n"
            + "    \"test\",\n"
            + "    \"hello\",\n"
            + "    \"mock\"\n"
            + "  ],\n"
            + "  \"favorites\": {\n"
            + "    \"game\": \"overwatch\",\n"
            + "    \"movie\": null\n"
            + "  }\n"
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
