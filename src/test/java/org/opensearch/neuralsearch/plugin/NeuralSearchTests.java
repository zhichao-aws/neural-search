/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import static org.mockito.Mockito.mock;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.OpenSearchQueryTestCase;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;

public class NeuralSearchTests extends OpenSearchQueryTestCase {

    public void testQuerySpecs() {
        NeuralSearch plugin = new NeuralSearch();
        List<SearchPlugin.QuerySpec<?>> querySpecs = plugin.getQueries();

        assertNotNull(querySpecs);
        assertFalse(querySpecs.isEmpty());
        assertTrue(querySpecs.stream().anyMatch(spec -> NeuralQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
        assertTrue(querySpecs.stream().anyMatch(spec -> HybridQueryBuilder.NAME.equals(spec.getName().getPreferredName())));
    }

    public void testQueryPhaseSearcher() {
        NeuralSearch plugin = new NeuralSearch();
        Optional<QueryPhaseSearcher> queryPhaseSearcherWithFeatureFlagDisabled = plugin.getQueryPhaseSearcher();

        assertNotNull(queryPhaseSearcherWithFeatureFlagDisabled);
        assertFalse(queryPhaseSearcherWithFeatureFlagDisabled.isEmpty());
        assertTrue(queryPhaseSearcherWithFeatureFlagDisabled.get() instanceof HybridQueryPhaseSearcher);

        initFeatureFlags();

        Optional<QueryPhaseSearcher> queryPhaseSearcher = plugin.getQueryPhaseSearcher();

        assertNotNull(queryPhaseSearcher);
        assertTrue(queryPhaseSearcher.isEmpty());
    }

    public void testProcessors() {
        NeuralSearch plugin = new NeuralSearch();
        Processor.Parameters processorParams = mock(Processor.Parameters.class);
        Map<String, Processor.Factory> processors = plugin.getProcessors(processorParams);
        assertNotNull(processors);
        assertNotNull(processors.get(TextEmbeddingProcessor.TYPE));
    }

    public void testSearchPhaseResultsProcessors() {
        NeuralSearch plugin = new NeuralSearch();
        SearchPipelinePlugin.Parameters parameters = mock(SearchPipelinePlugin.Parameters.class);
        Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor>> searchPhaseResultsProcessors = plugin
            .getSearchPhaseResultsProcessors(parameters);
        assertNotNull(searchPhaseResultsProcessors);
        assertEquals(1, searchPhaseResultsProcessors.size());
        assertTrue(searchPhaseResultsProcessors.containsKey("normalization-processor"));
        org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor> scoringProcessor = searchPhaseResultsProcessors.get(
            NormalizationProcessor.TYPE
        );
        assertTrue(scoringProcessor instanceof NormalizationProcessorFactory);
    }
}
