/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_SEARCH_HYBRID_SEARCH_DISABLED;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import lombok.extern.log4j.Log4j2;

import org.apache.lucene.analysis.Analyzer;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.index.analysis.PreBuiltAnalyzerProviderFactory;
import org.opensearch.index.analysis.PreConfiguredTokenizer;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.analysis.PreBuiltCacheFactory;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.analysis.DJLUtils;
import org.opensearch.neuralsearch.analysis.HFModelAnalyzer;
import org.opensearch.neuralsearch.analysis.HFModelAnalyzerProvider;
import org.opensearch.neuralsearch.analysis.HFModelTokenizer;
import org.opensearch.neuralsearch.analysis.HFModelTokenizerFactory;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.NeuralQueryEnricherProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.TextImageEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextImageEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralSparseQueryBuilder;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.neuralsearch.util.NeuralSearchClusterUtil;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.pipeline.SearchRequestProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Neural Search plugin class
 */
@Log4j2
public class NeuralSearch extends Plugin
    implements
        ActionPlugin,
        SearchPlugin,
        IngestPlugin,
        ExtensiblePlugin,
        SearchPipelinePlugin,
        AnalysisPlugin {
    private MLCommonsClientAccessor clientAccessor;
    private NormalizationProcessorWorkflow normalizationProcessorWorkflow;
    private final ScoreNormalizationFactory scoreNormalizationFactory = new ScoreNormalizationFactory();
    private final ScoreCombinationFactory scoreCombinationFactory = new ScoreCombinationFactory();

    @Override
    public Collection<Object> createComponents(
        final Client client,
        final ClusterService clusterService,
        final ThreadPool threadPool,
        final ResourceWatcherService resourceWatcherService,
        final ScriptService scriptService,
        final NamedXContentRegistry xContentRegistry,
        final Environment environment,
        final NodeEnvironment nodeEnvironment,
        final NamedWriteableRegistry namedWriteableRegistry,
        final IndexNameExpressionResolver indexNameExpressionResolver,
        final Supplier<RepositoriesService> repositoriesServiceSupplier
    ) {
        NeuralSearchClusterUtil.instance().initialize(clusterService);
        NeuralQueryBuilder.initialize(clientAccessor);
        NeuralSparseQueryBuilder.initialize(clientAccessor);
        normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());
        DJLUtils.buildDJLCachePath(environment.dataFiles()[0]);
        return List.of(clientAccessor);
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<TokenizerFactory>> getTokenizers() {
        return Map.of(HFModelTokenizer.NAME, HFModelTokenizerFactory::new);
    }

    @Override
    public List<PreConfiguredTokenizer> getPreConfiguredTokenizers() {
        List<PreConfiguredTokenizer> tokenizers = new ArrayList<>();
        tokenizers.add(PreConfiguredTokenizer.singleton(HFModelTokenizer.NAME, HFModelTokenizerFactory::createDefault));
        return tokenizers;
    }

    @Override
    public Map<String, AnalysisModule.AnalysisProvider<AnalyzerProvider<? extends Analyzer>>> getAnalyzers() {
        return Map.of(HFModelAnalyzer.NAME, HFModelAnalyzerProvider::new);
    }

    @Override
    public List<PreBuiltAnalyzerProviderFactory> getPreBuiltAnalyzerProviderFactories() {
        List<PreBuiltAnalyzerProviderFactory> factories = new ArrayList<>();
        factories.add(
                new PreBuiltAnalyzerProviderFactory(HFModelAnalyzer.NAME, PreBuiltCacheFactory.CachingStrategy.ONE, HFModelAnalyzer::new)
        );
        return factories;
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent),
            new QuerySpec<>(HybridQueryBuilder.NAME, HybridQueryBuilder::new, HybridQueryBuilder::fromXContent),
            new QuerySpec<>(NeuralSparseQueryBuilder.NAME, NeuralSparseQueryBuilder::new, NeuralSparseQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        return Map.of(
            TextEmbeddingProcessor.TYPE,
            new TextEmbeddingProcessorFactory(clientAccessor, parameters.env),
            SparseEncodingProcessor.TYPE,
            new SparseEncodingProcessorFactory(clientAccessor, parameters.env),
            TextImageEmbeddingProcessor.TYPE,
            new TextImageEmbeddingProcessorFactory(clientAccessor, parameters.env, parameters.ingestService.getClusterService())
        );
    }

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        // we're using "is_disabled" flag as there are no proper implementation of FeatureFlags.isDisabled(). Both
        // cases when flag is not set or it is "false" are interpretted in the same way. In such case core is reading
        // the actual value from settings.
        if (FeatureFlags.isEnabled(NEURAL_SEARCH_HYBRID_SEARCH_DISABLED.getKey())) {
            log.info(
                "Not registering hybrid query phase searcher because feature flag [{}] is disabled",
                NEURAL_SEARCH_HYBRID_SEARCH_DISABLED.getKey()
            );
            return Optional.empty();
        }
        log.info("Registering hybrid query phase searcher with feature flag [{}]", NEURAL_SEARCH_HYBRID_SEARCH_DISABLED.getKey());
        return Optional.of(new HybridQueryPhaseSearcher());
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchPhaseResultsProcessor>> getSearchPhaseResultsProcessors(
        Parameters parameters
    ) {
        return Map.of(
            NormalizationProcessor.TYPE,
            new NormalizationProcessorFactory(normalizationProcessorWorkflow, scoreNormalizationFactory, scoreCombinationFactory)
        );
    }

    @Override
    public List<Setting<?>> getSettings() {
        return List.of(NEURAL_SEARCH_HYBRID_SEARCH_DISABLED);
    }

    @Override
    public Map<String, org.opensearch.search.pipeline.Processor.Factory<SearchRequestProcessor>> getRequestProcessors(
        Parameters parameters
    ) {
        return Map.of(NeuralQueryEnricherProcessor.TYPE, new NeuralQueryEnricherProcessor.Factory());
    }
}
