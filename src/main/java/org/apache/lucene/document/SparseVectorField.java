/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.lucene.document;

import java.io.IOException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.similarities.Similarity.SimScorer;

/**
 * {@link Field} that can be used to store static scoring factors into documents. This is mostly
 * inspired from the work from Nick Craswell, Stephen Robertson, Hugo Zaragoza and Michael Taylor.
 * Relevance weighting for query independent evidence. Proceedings of the 28th annual international
 * ACM SIGIR conference on Research and development in information retrieval. August 15-19, 2005,
 * Salvador, Brazil.
 *
 * <p>Feature values are internally encoded as term frequencies. Putting feature queries as {@link
 * org.apache.lucene.search.BooleanClause.Occur#SHOULD} clauses of a {@link BooleanQuery} allows to
 * combine query-dependent scores (eg. BM25) with query-independent scores using a linear
 * combination. The fact that feature values are stored as frequencies also allows search logic to
 * efficiently skip documents that can't be competitive when total hit counts are not requested.
 * This makes it a compelling option compared to storing such factors eg. in a doc-value field.
 *
 * <p>This field may only store factors that are positively correlated with the final score, like
 * pagerank. In case of factors that are inversely correlated with the score like url length, the
 * inverse of the scoring factor should be stored, ie. {@code 1/urlLength}.
 *
 * <p>This field only considers the top 9 significant bits for storage efficiency which allows to
 * store them on 16 bits internally. In practice this limitation means that values are stored with a
 * relative precision of 2<sup>-8</sup> = 0.00390625.
 *
 * <p>Given a scoring factor {@code S > 0} and its weight {@code w > 0}, there are four ways that S
 * can be turned into a score:
 *
 * <pre class="prettyprint">
 * Query query = new BooleanQuery.Builder()
 *     .add(new TermQuery(new Term("body", "apache")), Occur.SHOULD)
 *     .add(new TermQuery(new Term("body", "lucene")), Occur.SHOULD)
 *     .build();
 * Query boost = SparseVectorField.newSaturationQuery("features", "pagerank");
 * Query boostedQuery = new BooleanQuery.Builder()
 *     .add(query, Occur.MUST)
 *     .add(boost, Occur.SHOULD)
 *     .build();
 * TopDocs topDocs = searcher.search(boostedQuery, 10);
 * </pre>
 *
 * @lucene.experimental
 */
public final class SparseVectorField extends Field {

    private static final FieldType FIELD_TYPE = new FieldType();

    static {
        FIELD_TYPE.setTokenized(false);
        FIELD_TYPE.setOmitNorms(true);
        FIELD_TYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS);
    }

    public static int quantize(float value, float maxScore, int bits) {
        int scale = (1 << bits) - 1;
        value = Math.min(value, maxScore);
        int quantized = Math.round((value / maxScore) * scale);
        return quantized;
    }

    public static float dequantize(float freq, float maxScore, int bits) {
        int tf = (int) freq;
        int scale = (1 << bits) - 1;
        if (tf >= scale) {
            return maxScore;
        }
        float value = ((float) tf / scale) * maxScore;
        return value;
    }

    private int featureValueBits;

    /**
     * Create a feature.
     *
     * @param fieldName The name of the field to store the information into. All features may be
     *     stored in the same field.
     * @param featureName The name of the feature, eg. 'pagerank`. It will be indexed as a term.
     * @param featureValueBits The value of the feature, must be a positive, finite, normal float.
     */
    public SparseVectorField(String fieldName, String featureName, int featureValueBits) {
        super(fieldName, featureName, FIELD_TYPE);
        setFeatureValueBits(featureValueBits);
    }

    /** Update the feature value of this field. */
    public void setFeatureValueBits(int featureValueBits) {
        if (featureValueBits <= 0) {
            throw new IllegalArgumentException(
                "featureValue must be a positive normal float, got: "
                    + featureValueBits
                    + " for feature "
                    + fieldsData
                    + " on field "
                    + name
                    + " which is less than the minimum positive normal float: "
                    + Float.MIN_NORMAL
            );
        }
        this.featureValueBits = featureValueBits;
    }

    @Override
    public TokenStream tokenStream(Analyzer analyzer, TokenStream reuse) {
        FeatureTokenStream stream;
        if (reuse instanceof FeatureTokenStream) {
            stream = (FeatureTokenStream) reuse;
        } else {
            stream = new FeatureTokenStream();
        }

        stream.setValues((String) fieldsData, featureValueBits);
        return stream;
    }

    /**
     * This is useful if you have multiple features sharing a name and you want to take action to
     * deduplicate them.
     *
     * @return the feature value of this field.
     */
    public int getFeatureValueBits() {
        return featureValueBits;
    }

    private static final class FeatureTokenStream extends TokenStream {
        private final CharTermAttribute termAttribute = addAttribute(CharTermAttribute.class);
        private final TermFrequencyAttribute freqAttribute = addAttribute(TermFrequencyAttribute.class);
        private boolean used = true;
        private String value = null;
        private int freq = 0;

        private FeatureTokenStream() {}

        /** Sets the values */
        void setValues(String value, int freq) {
            this.value = value;
            this.freq = freq;
        }

        @Override
        public boolean incrementToken() {
            if (used) {
                return false;
            }
            clearAttributes();
            termAttribute.append(value);
            freqAttribute.setTermFrequency(freq);
            used = true;
            return true;
        }

        @Override
        public void reset() {
            used = false;
        }

        @Override
        public void close() {
            value = null;
        }
    }

    abstract static class SparseVectorFunction {
        abstract SimScorer scorer(float w);

        abstract Explanation explain(String field, String feature, float w, int freq);

        SparseVectorFunction rewrite(IndexSearcher indexSearcher) throws IOException {
            return this;
        }
    }

    static final class LinearFunction extends SparseVectorFunction {
        private float maxScore;
        private int bits;

        LinearFunction(float maxScore, int bits) {
            this.maxScore = maxScore;
            this.bits = bits;
        }

        @Override
        SimScorer scorer(float w) {
            return new SimScorer() {
                @Override
                public float score(float freq, long norm) {
                    return (w * dequantize(freq, maxScore, bits));
                }
            };
        }

        @Override
        Explanation explain(String field, String feature, float w, int freq) {
            float featureValue = dequantize(freq, maxScore, bits);
            float score = scorer(w).score(freq, 1L);
            return Explanation.match(
                score,
                "Linear function on the " + field + " field for the " + feature + " feature, computed as w * S from:",
                Explanation.match(w, "w, weight of this function"),
                Explanation.match(featureValue, "S, feature value")
            );
        }

        @Override
        public String toString() {
            return "LinearFunction";
        }

        @Override
        public int hashCode() {
            return getClass().hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            return true;
        }
    }

    /**
     * Given that IDFs are logs, similarities that incorporate term freq and document length in sane
     * (ie. saturated) ways should have their score bounded by a log. So we reject weights that are
     * too high as it would mean that this clause would completely dominate ranking, removing the need
     * for query-dependent scores.
     */
    private static final float MAX_WEIGHT = Long.SIZE;

    /**
     * Return a new {@link Query} that will score documents as {@code weight * S} where S is the value
     * of the static feature.
     *
     * @param fieldName field that stores features
     * @param featureName name of the feature
     * @param weight weight to give to this feature, must be in (0,64]
     * @throws IllegalArgumentException if weight is not in (0,64]
     */
    public static Query newLinearQuery(String fieldName, String featureName, float weight, float maxScore, int bits) {
        if (weight <= 0 || weight > MAX_WEIGHT) {
            throw new IllegalArgumentException("weight must be in (0, " + MAX_WEIGHT + "], got: " + weight);
        }
        Query q = new SparseVectorQuery(fieldName, featureName, new LinearFunction(maxScore, bits));
        if (weight != 1f) {
            q = new BoostQuery(q, weight);
        }
        return q;
    }
}
