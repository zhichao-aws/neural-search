/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.util.Map;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.analysis.Tokenizer;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AbstractTokenizerFactory;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

/**
 * Factory class for creating HFModelTokenizer instances.
 * Handles the initialization and configuration of tokenizers based on index settings.
 */
@Log4j2
public class HFModelTokenizerFactory extends AbstractTokenizerFactory {
    /**
     * Atomically loads the HF tokenizer in a lazy fashion once the outer class accesses the static final set the first time.;
     */
    private static class DefaultTokenizerHolder {
        static final HuggingFaceTokenizer TOKENIZER;
        static final Map<String, Float> TOKEN_WEIGHTS;
        static private final String TOKENIZER_PATH = "/analysis/tokenizer_en.json";
        static private final String TOKEN_WEIGHTS_PATH = "/analysis/token_weights_en.txt";

        static {
            try {
                TOKENIZER = DJLUtils.buildHuggingFaceTokenizer(TOKENIZER_PATH);
                TOKEN_WEIGHTS = DJLUtils.fetchTokenWeights(TOKEN_WEIGHTS_PATH);
            } catch (Exception e) {
                throw new RuntimeException("Failed to initialize default hf_model_tokenizer", e);
            }
        }
    }

    /**
     * Creates a default tokenizer instance with predefined settings.
     * @return A new HFModelTokenizer instance with default HuggingFaceTokenizer.
     */
    public static Tokenizer createDefault() {
        return new HFModelTokenizer(DefaultTokenizerHolder.TOKENIZER, DefaultTokenizerHolder.TOKEN_WEIGHTS);
    }

    public HFModelTokenizerFactory(IndexSettings indexSettings, Environment environment, String name, Settings settings) {
        // For custom tokenizer, the factory is created during IndexModule.newIndexService
        // And can be accessed via indexService.getIndexAnalyzers()
        super(indexSettings, settings, name);
    }

    @Override
    public Tokenizer create() {
        // the create method will be called for every single analyze request
        return new HFModelTokenizer(DefaultTokenizerHolder.TOKENIZER, DefaultTokenizerHolder.TOKEN_WEIGHTS);
    }
}
