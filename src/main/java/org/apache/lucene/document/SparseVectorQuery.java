/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.apache.lucene.document;

import org.apache.lucene.index.ImpactsEnum;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafSimScorer;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermScorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.similarities.Similarity;
import org.apache.lucene.util.BytesRef;

import java.io.IOException;
import java.util.Objects;

public class SparseVectorQuery extends Query {
    private final String fieldName;
    private final String featureName;
    private final SparseVectorField.SparseVectorFunction function;

    SparseVectorQuery(String fieldName, String featureName, SparseVectorField.SparseVectorFunction function) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.featureName = Objects.requireNonNull(featureName);
        this.function = Objects.requireNonNull(function);
    }

    @Override
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        SparseVectorField.SparseVectorFunction rewritten = function.rewrite(indexSearcher);
        if (function != rewritten) {
            return new SparseVectorQuery(fieldName, featureName, rewritten);
        }
        return super.rewrite(indexSearcher);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        SparseVectorQuery that = (SparseVectorQuery) obj;
        return Objects.equals(fieldName, that.fieldName)
            && Objects.equals(featureName, that.featureName)
            && Objects.equals(function, that.function);
    }

    @Override
    public int hashCode() {
        int h = getClass().hashCode();
        h = 31 * h + fieldName.hashCode();
        h = 31 * h + featureName.hashCode();
        h = 31 * h + function.hashCode();
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        if (!scoreMode.needsScores()) {
            // We don't need scores (e.g. for faceting), and since features are stored as terms,
            // allow TermQuery to optimize in this case
            TermQuery tq = new TermQuery(new Term(fieldName, featureName));
            return searcher.rewrite(tq).createWeight(searcher, scoreMode, boost);
        }

        return new Weight(this) {

            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                return false;
            }

            @Override
            public Explanation explain(LeafReaderContext context, int doc) throws IOException {
                String desc = "weight(" + getQuery() + " in " + doc + ") [" + function + "]";

                Terms terms = context.reader().terms(fieldName);
                if (terms == null) {
                    return Explanation.noMatch(desc + ". Field " + fieldName + " doesn't exist.");
                }
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(new BytesRef(featureName)) == false) {
                    return Explanation.noMatch(desc + ". Feature " + featureName + " doesn't exist.");
                }

                PostingsEnum postings = termsEnum.postings(null, PostingsEnum.FREQS);
                if (postings.advance(doc) != doc) {
                    return Explanation.noMatch(desc + ". Feature " + featureName + " isn't set.");
                }

                return function.explain(fieldName, featureName, boost, postings.freq());
            }

            @Override
            public ScorerSupplier scorerSupplier(LeafReaderContext context) throws IOException {
                final Weight thisWeight = this;
                Terms terms = Terms.getTerms(context.reader(), fieldName);
                TermsEnum termsEnum = terms.iterator();
                if (termsEnum.seekExact(new BytesRef(featureName)) == false) {
                    return null;
                }
                final int docFreq = termsEnum.docFreq();

                return new ScorerSupplier() {

                    private boolean topLevelScoringClause = false;

                    @Override
                    public Scorer get(long leadCost) throws IOException {
                        final Similarity.SimScorer scorer = function.scorer(boost);
                        final LeafSimScorer simScorer = new LeafSimScorer(scorer, context.reader(), fieldName, false);
                        final ImpactsEnum impacts = termsEnum.impacts(PostingsEnum.FREQS);
                        return new TermScorer(thisWeight, impacts, simScorer, topLevelScoringClause);
                    }

                    @Override
                    public long cost() {
                        return docFreq;
                    }

                    @Override
                    public void setTopLevelScoringClause() throws IOException {
                        topLevelScoringClause = true;
                    }
                };
            }

            @Override
            public Scorer scorer(LeafReaderContext context) throws IOException {
                ScorerSupplier supplier = scorerSupplier(context);
                if (supplier == null) {
                    return null;
                }
                return supplier.get(Long.MAX_VALUE);
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
        return "SparseVectorQuery(field=" + fieldName + ", feature=" + featureName + ", function=" + function + ")";
    }
}
