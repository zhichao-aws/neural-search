/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor.factory;

import static org.opensearch.ingest.ConfigurationUtils.readDoubleProperty;
import static org.opensearch.ingest.ConfigurationUtils.readMap;
import static org.opensearch.ingest.ConfigurationUtils.readOptionalStringProperty;
import static org.opensearch.ingest.ConfigurationUtils.readStringProperty;
import static org.opensearch.neuralsearch.processor.SparseEncodingProcessor.TYPE;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.MODEL_ID_FIELD;
import static org.opensearch.neuralsearch.processor.TextEmbeddingProcessor.FIELD_MAP_FIELD;

import java.util.Map;

import org.opensearch.cluster.service.ClusterService;
import org.opensearch.env.Environment;
import org.opensearch.ingest.AbstractBatchingProcessor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;

import lombok.extern.log4j.Log4j2;

/**
 * Factory for sparse encoding ingest processor for ingestion pipeline. Instantiates processor based on user provided input.
 */
@Log4j2
public class SparseEncodingProcessorFactory extends AbstractBatchingProcessor.Factory {
    private final MLCommonsClientAccessor clientAccessor;
    private final Environment environment;
    private final ClusterService clusterService;

    public SparseEncodingProcessorFactory(MLCommonsClientAccessor clientAccessor, Environment environment, ClusterService clusterService) {
        super(TYPE);
        this.clientAccessor = clientAccessor;
        this.environment = environment;
        this.clusterService = clusterService;
    }

    @Override
    protected AbstractBatchingProcessor newProcessor(String tag, String description, int batchSize, Map<String, Object> config) {
        String modelId = readStringProperty(TYPE, tag, config, MODEL_ID_FIELD);
        Map<String, Object> fieldMap = readMap(TYPE, tag, config, FIELD_MAP_FIELD);
        String pruningType = readOptionalStringProperty(TYPE, tag, config, "pruning_type");
        float lambda = readDoubleProperty(TYPE, tag, config, "pruning_number").floatValue();

        return new SparseEncodingProcessor(
            tag,
            description,
            batchSize,
            modelId,
            fieldMap,
            pruningType,
            lambda,
            clientAccessor,
            environment,
            clusterService
        );
    }
}
