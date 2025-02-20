/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.util.function.Supplier;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

/**
 * Custom Lucene Analyzer that uses the HFModelTokenizer for text analysis.
 * Provides a way to process text using Hugging Face models within OpenSearch.
 */
public class HFModelAnalyzer extends Analyzer {
    public static final String NAME = "hf_model_tokenizer";
    Supplier<Tokenizer> tokenizerSupplier;

    public HFModelAnalyzer() {
        this.tokenizerSupplier = HFModelTokenizerFactory::createDefault;
    }

    HFModelAnalyzer(Supplier<Tokenizer> tokenizerSupplier) {
        this.tokenizerSupplier = tokenizerSupplier;
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName) {
        final Tokenizer src = tokenizerSupplier.get();
        return new TokenStreamComponents(src, src);
    }
}
