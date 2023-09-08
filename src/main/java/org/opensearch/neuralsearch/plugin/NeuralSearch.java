/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.neuralsearch.plugin;

import static org.opensearch.neuralsearch.settings.NeuralSearchSettings.NEURAL_SEARCH_HYBRID_SEARCH_ENABLED;

import java.util.*;
import java.util.function.Supplier;

import lombok.extern.log4j.Log4j2;

import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.ingest.Processor;
import org.opensearch.ml.client.MachineLearningNodeClient;
import org.opensearch.neuralsearch.index.mapper.SparseVectorMapper;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessor;
import org.opensearch.neuralsearch.processor.NormalizationProcessorWorkflow;
import org.opensearch.neuralsearch.processor.SparseEncodingProcessor;
import org.opensearch.neuralsearch.processor.TextEmbeddingProcessor;
import org.opensearch.neuralsearch.processor.combination.ScoreCombinationFactory;
import org.opensearch.neuralsearch.processor.combination.ScoreCombiner;
import org.opensearch.neuralsearch.processor.factory.NormalizationProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;
import org.opensearch.neuralsearch.processor.factory.SparseEncodingProcessorFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizationFactory;
import org.opensearch.neuralsearch.processor.normalization.ScoreNormalizer;
import org.opensearch.neuralsearch.query.HybridQueryBuilder;
import org.opensearch.neuralsearch.query.NeuralQueryBuilder;
import org.opensearch.neuralsearch.query.sparse.SparseQueryBuilder;
import org.opensearch.neuralsearch.search.query.HybridQueryPhaseSearcher;
import org.opensearch.plugins.ActionPlugin;
import org.opensearch.plugins.ExtensiblePlugin;
import org.opensearch.plugins.IngestPlugin;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPipelinePlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.repositories.RepositoriesService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.pipeline.SearchPhaseResultsProcessor;
import org.opensearch.search.query.QueryPhaseSearcher;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.watcher.ResourceWatcherService;

/**
 * Neural Search plugin class
 */
@Log4j2
public class NeuralSearch extends Plugin implements
        ActionPlugin,
        SearchPlugin,
        IngestPlugin,
        ExtensiblePlugin,
        SearchPipelinePlugin,
        MapperPlugin {
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
        NeuralQueryBuilder.initialize(clientAccessor);
        SparseQueryBuilder.initialize(clientAccessor);
        normalizationProcessorWorkflow = new NormalizationProcessorWorkflow(new ScoreNormalizer(), new ScoreCombiner());
        return List.of(clientAccessor);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return Arrays.asList(
            new QuerySpec<>(NeuralQueryBuilder.NAME, NeuralQueryBuilder::new, NeuralQueryBuilder::fromXContent),
            new QuerySpec<>(HybridQueryBuilder.NAME, HybridQueryBuilder::new, HybridQueryBuilder::fromXContent),
            new QuerySpec<>(SparseQueryBuilder.NAME, SparseQueryBuilder::new, SparseQueryBuilder::fromXContent)
        );
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        return Collections.singletonMap(
                SparseVectorMapper.CONTENT_TYPE,
                SparseVectorMapper.PARSER
        );
    }

    @Override
    public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
        clientAccessor = new MLCommonsClientAccessor(new MachineLearningNodeClient(parameters.client));
        Map<String, Processor.Factory> allProcessors = new HashMap<>();
        allProcessors.put(TextEmbeddingProcessor.TYPE, new TextEmbeddingProcessorFactory(clientAccessor, parameters.env));
        allProcessors.put(SparseEncodingProcessor.TYPE, new SparseEncodingProcessorFactory(clientAccessor, parameters.env));
        return allProcessors;
    }

    @Override
    public Optional<QueryPhaseSearcher> getQueryPhaseSearcher() {
        if (FeatureFlags.isEnabled(NEURAL_SEARCH_HYBRID_SEARCH_ENABLED.getKey())) {
            log.info("Registering hybrid query phase searcher with feature flag [{}]", NEURAL_SEARCH_HYBRID_SEARCH_ENABLED.getKey());
            return Optional.of(new HybridQueryPhaseSearcher());
        }
        log.info(
            "Not registering hybrid query phase searcher because feature flag [{}] is disabled",
            NEURAL_SEARCH_HYBRID_SEARCH_ENABLED.getKey()
        );
        // we want feature be disabled by default due to risk of colliding and breaking concurrent search in core
        return Optional.empty();
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
        return List.of(NEURAL_SEARCH_HYBRID_SEARCH_ENABLED);
    }
}
