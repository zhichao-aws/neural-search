/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Objects;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.ImpactsDISI;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity.SimScorer;
import org.apache.lucene.util.BytesRef;

final class FeatureQuery extends Query {

    private final String fieldName;
    private final String featureName;

    FeatureQuery(String fieldName, String featureName) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.featureName = Objects.requireNonNull(featureName);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        FeatureQuery that = (FeatureQuery) obj;
        return Objects.equals(fieldName, that.fieldName)
                && Objects.equals(featureName, that.featureName);
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + fieldName.hashCode();
        h = 31 * h + featureName.hashCode();
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost)
            throws IOException {
        if (!scoreMode.needsScores()) {
            // We don't need scores (e.g. for faceting), and since features are stored as terms,
            // allow TermQuery to optimize in this case
            TermQuery tq = new TermQuery(new Term(fieldName, featureName));
            return searcher.rewrite(tq).createWeight(searcher, scoreMode, boost);
        }

        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return true;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                return null;
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                Terms terms = Terms.getTerms(context.reader(), fieldName);
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(new BytesRef(featureName)) == false) {
                    return null;
                }

                final SimScorer scorer = new SimScorer() {
                    @Override
                    public float score(float freq, long norm) {
                        return boost*decodeFeatureValue(freq);
                    }
                };
                final ImpactsEnum impacts = termsEnum.impacts(PostingsEnum.FREQS);
                final ImpactsDISI impactsDisi = new ImpactsDISI(impacts, impacts, scorer);

                return new Scorer(this) {

                    @Override
                    public int docID() {
                        return impacts.docID();
                    }

                    @Override
                    public float score() throws IOException {
                        return scorer.score(impacts.freq(), 1L);
                    }

                    @Override
                    public DocIdSetIterator iterator() {
                        return impactsDisi;
                    }

                    @Override
                    public int advanceShallow(int target) throws IOException {
                        return impactsDisi.advanceShallow(target);
                    }

                    @Override
                    public float getMaxScore(int upTo) throws IOException {
                        return impactsDisi.getMaxScore(upTo);
                    }

                    @Override
                    public void setMinCompetitiveScore(float minScore) {
                        impactsDisi.setMinCompetitiveScore(minScore);
                    }
                };
            }
        };
    }

    @Override
    public void visit(QueryVisitor visitor) {
        if (visitor.acceptField(fieldName)) {
            visitor.visitLeaf(this);
        }
    }

    @Override
    public String toString(String field) {
        return "FeatureQuery(field="
                + fieldName
                + ", feature="
                + featureName
                + ")";
    }

    static final int MAX_FREQ = Float.floatToIntBits(Float.MAX_VALUE) >>> 15;
    static float decodeFeatureValue(float freq) {
        if (freq > MAX_FREQ) {
            // This is never used in practice but callers of the SimScorer API might
            // occasionally call it on eg. Float.MAX_VALUE to compute the max score
            // so we need to be consistent.
//             return Float.MAX_VALUE;
            return 3f;
        }
        int tf = (int) freq; // lossless
        int featureBits = tf << 15;
//        return Math.max(Float.intBitsToFloat(featureBits), 1f);
        return Float.intBitsToFloat(featureBits);
    }
}
