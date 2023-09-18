/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.processor;

import lombok.extern.log4j.Log4j2;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.util.TokenWeightUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;


@Log4j2
public class SparseEncodingProcessor extends NLPProcessor {

    public static final String TYPE = "sparse_encoding";
    public static final String LIST_TYPE_NESTED_MAP_KEY = "sparse_encoding";

    public SparseEncodingProcessor(
        String tag,
        String description,
        String modelId,
        Map<String, Object> fieldMap,
        MLCommonsClientAccessor clientAccessor,
        Environment environment
    ) {
        super(tag, description, TYPE, LIST_TYPE_NESTED_MAP_KEY, modelId, fieldMap, clientAccessor, environment);
    }

    @Override
    public void doExecute(
        IngestDocument ingestDocument,
        Map<String, Object> ProcessMap,
        List<String> inferenceList,
        BiConsumer<IngestDocument,
        Exception> handler
    ) {
        mlCommonsClientAccessor.inferenceSentencesWithMapResult(this.modelId, inferenceList, ActionListener.wrap(resultMaps -> {
            setVectorFieldsToDocument(ingestDocument, ProcessMap, TokenWeightUtil.fetchListOfTokenWeightMap(resultMaps));
            handler.accept(ingestDocument, null);
        }, e -> { handler.accept(null, e); }));
    }
}
