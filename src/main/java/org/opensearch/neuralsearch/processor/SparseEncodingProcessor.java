/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.pruning.PruningUtils;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import lombok.extern.log4j.Log4j2;

/**
 * This processor is used for user input data text sparse encoding processing, model_id can be used to indicate which model user use,
 * and field_map can be used to indicate which fields needs text embedding and the corresponding keys for the sparse encoding results.
 */
@Log4j2
public final class SparseEncodingProcessor extends InferenceProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "sparse_encoding";

    private String pruningType;
    private float lambda;

    public SparseEncodingProcessor(
        String tag,
        String description,
        int batchSize,
        String modelId,
        Map<String, Object> fieldMap,
        String pruningType,
        float lambda,
        MLCommonsClientAccessor clientAccessor,
        Environment environment,
        ClusterService clusterService
    ) {
        super(tag, description, batchSize, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment, clusterService);
        this.pruningType = pruningType;
        this.lambda = lambda;
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> processMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument, Exception> handler
    ) {
        assert processMap.size() == 1;
        String key = (String) processMap.keySet().toArray()[0];
        if (ingestDocument.getSourceAndMetadata().containsKey(key)) {
            // todo: validate the data is not malformed here
            ingestDocument.getSourceAndMetadata()
                .put(
                    key,
                    PruningUtils.pruningSparseVector(
                        pruningType,
                        lambda,
                        TokenWeightUtil.buildTokenWeightMap(ingestDocument.getSourceAndMetadata().get(key))
                    )
                );
            handler.accept(ingestDocument, null);
            return;
        }
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            setVectorFieldsToDocument(ingestDocument, processMap, TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps));
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }

    @Override
    public void doBatchExecute(List<String> inferenceList, Consumer<List<?>> handler, Consumer<Exception> onException) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(
            this.modelId,
            inferenceList,
            ActionListener.wrap(resultMaps -> handler.accept(TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps)), onException)
        );
    }
}
