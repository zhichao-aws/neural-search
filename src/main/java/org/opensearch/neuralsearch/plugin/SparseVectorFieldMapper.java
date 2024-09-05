/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.plugin;

import org.apache.lucene.document.SparseVectorField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.Query;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.index.fielddata.IndexFieldData;
import org.opensearch.index.mapper.FieldMapper;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.ParametrizedFieldMapper;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.SourceValueFetcher;
import org.opensearch.index.mapper.TextSearchInfo;
import org.opensearch.index.mapper.ValueFetcher;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class SparseVectorFieldMapper extends ParametrizedFieldMapper {

    public static final String CONTENT_TYPE = "sparse_vector";

    private static SparseVectorFieldType ft(FieldMapper in) {
        return ((SparseVectorFieldMapper) in).fieldType();
    }

    public static class Builder extends ParametrizedFieldMapper.Builder {

        private final Parameter<Float> maxScore = Parameter.floatParam("max_score", false, m -> ft(m).maxScore, 2.0f);
        private final Parameter<Integer> bits = Parameter.intParam("bits", false, m -> ft(m).bits, 8);
        private final Parameter<Map<String, String>> meta = Parameter.metaParam();

        public Builder(String name) {
            super(name);
            builder = this;
        }

        @Override
        protected List<Parameter<?>> getParameters() {
            return List.of(meta, maxScore, bits);
        }

        @Override
        public SparseVectorFieldMapper build(BuilderContext context) {
            return new SparseVectorFieldMapper(
                name,
                new SparseVectorFieldType(buildFullName(context), meta.getValue(), maxScore.getValue(), bits.getValue()),
                multiFieldsBuilder.build(this, context),
                copyTo.build(),
                maxScore.getValue(),
                bits.getValue()
            );
        }
    }

    public static final TypeParser PARSER = new TypeParser((n, c) -> new Builder(n));

    public static final class SparseVectorFieldType extends MappedFieldType {

        private final float maxScore;
        private final int bits;

        public SparseVectorFieldType(String name, Map<String, String> meta, float maxScore, int bits) {
            super(name, false, false, false, TextSearchInfo.NONE, meta);
            setIndexAnalyzer(Lucene.KEYWORD_ANALYZER);
            this.maxScore = maxScore;
            this.bits = bits;
        }

        public float getMaxScore() {
            return maxScore;
        }

        public int getBits() {
            return bits;
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query existsQuery(QueryShardContext context) {
            throw new IllegalArgumentException("[rank_features] fields do not support [exists] queries");
        }

        @Override
        public IndexFieldData.Builder fielddataBuilder(String fullyQualifiedIndexName, Supplier<SearchLookup> searchLookup) {
            throw new IllegalArgumentException("[rank_features] fields do not support sorting, scripting or aggregating");
        }

        @Override
        public ValueFetcher valueFetcher(QueryShardContext context, SearchLookup searchLookup, String format) {
            return SourceValueFetcher.identity(name(), context, format);
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new IllegalArgumentException("Queries on [rank_features] fields are not supported");
        }
    }

    private final float maxScore;
    private final int bits;

    private SparseVectorFieldMapper(
        String simpleName,
        MappedFieldType mappedFieldType,
        MultiFields multiFields,
        CopyTo copyTo,
        float maxScore,
        int bits
    ) {
        super(simpleName, mappedFieldType, multiFields, copyTo);
        assert fieldType.indexOptions().compareTo(IndexOptions.DOCS_AND_FREQS) <= 0;
        this.maxScore = maxScore;
        this.bits = bits;
    }

    @Override
    public ParametrizedFieldMapper.Builder getMergeBuilder() {
        return new Builder(simpleName()).init(this);
    }

    @Override
    protected SparseVectorFieldMapper clone() {
        return (SparseVectorFieldMapper) super.clone();
    }

    @Override
    public SparseVectorFieldType fieldType() {
        return (SparseVectorFieldType) super.fieldType();
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        if (context.externalValueSet()) {
            throw new IllegalArgumentException("[rank_features] fields can't be used in multi-fields");
        }

        if (context.parser().currentToken() != Token.START_OBJECT) {
            throw new IllegalArgumentException(
                "[rank_features] fields must be json objects, expected a START_OBJECT but got: " + context.parser().currentToken()
            );
        }

        String feature = null;
        for (Token token = context.parser().nextToken(); token != Token.END_OBJECT; token = context.parser().nextToken()) {
            if (token == Token.FIELD_NAME) {
                feature = context.parser().currentName();
            } else if (token == Token.VALUE_NULL) {
                // ignore feature, this is consistent with numeric fields
            } else if (token == Token.VALUE_NUMBER || token == Token.VALUE_STRING) {
                final String key = name() + "." + feature;
                float value = context.parser().floatValue(true);
                if (context.doc().getByKey(key) != null) {
                    throw new IllegalArgumentException(
                        "[rank_features] fields do not support indexing multiple values for the same "
                            + "rank feature ["
                            + key
                            + "] in the same document"
                    );
                }
                if (bits == 0) continue;
                int freq = SparseVectorField.quantize(value, maxScore, bits);
                if (freq > 0) context.doc().addWithKey(key, new SparseVectorField(name(), feature, freq));
            } else {
                throw new IllegalArgumentException(
                    "[rank_features] fields take hashes that map a feature to a strictly positive "
                        + "float, but got unexpected token "
                        + token
                );
            }
        }
    }

    @Override
    protected void parseCreateField(ParseContext context) {
        throw new AssertionError("parse is implemented directly");
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

}
