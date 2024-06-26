/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.processor;

import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.isNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.isA;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Arrays;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchParseException;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.env.Environment;
import org.opensearch.index.mapper.IndexFieldMapper;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.IngestDocumentWrapper;
import org.opensearch.ingest.Processor;
import org.opensearch.neuralsearch.ml.MLCommonsClientAccessor;
import org.opensearch.neuralsearch.processor.factory.TextEmbeddingProcessorFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import lombok.SneakyThrows;

public class TextEmbeddingProcessorTests extends InferenceProcessorTestCase {

    @Mock
    private MLCommonsClientAccessor mlCommonsClientAccessor;

    @Mock
    private Environment environment;

    private ClusterService clusterService = mock(ClusterService.class, RETURNS_DEEP_STUBS);

    @InjectMocks
    private TextEmbeddingProcessorFactory textEmbeddingProcessorFactory;
    private static final String PROCESSOR_TAG = "mockTag";
    private static final String DESCRIPTION = "mockDescription";

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        Settings settings = Settings.builder().put("index.mapping.depth.limit", 20).build();
        when(clusterService.state().metadata().index(anyString()).getSettings()).thenReturn(settings);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel2MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(
            TextEmbeddingProcessor.FIELD_MAP_FIELD,
            ImmutableMap.of("key1", ImmutableMap.of("test1", "test1_knn"), "key2", ImmutableMap.of("test3", "test3_knn"))
        );
        return textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithLevel1MapConfig() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1_knn", "key2", "key2_knn"));
        return textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapError_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        Map<String, String> fieldMap = new HashMap<>();
        fieldMap.put(null, "key1Mapped");
        fieldMap.put("key2", "key2Mapped");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        try {
            textEmbeddingProcessorFactory.create(registry, TextEmbeddingProcessor.TYPE, DESCRIPTION, config);
        } catch (IllegalArgumentException e) {
            assertEquals("Unable to create the processor as field_map has invalid key or value", e.getMessage());
        }
    }

    @SneakyThrows
    public void testTextEmbeddingProcessConstructor_whenConfigMapEmpty_throwIllegalArgumentException() {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        try {
            textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        } catch (OpenSearchParseException e) {
            assertEquals("[field_map] required property is missing", e.getMessage());
        }
    }

    public void testExecute_successful() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    @SneakyThrows
    public void testExecute_whenInferenceThrowInterruptedException_throwRuntimeException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            accessor,
            environment,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        doThrow(new RuntimeException()).when(accessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(RuntimeException.class));
    }

    @SneakyThrows
    public void testExecute_whenInferenceTextListEmpty_SuccessWithoutEmbedding() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        Map<String, Processor.Factory> registry = new HashMap<>();
        MLCommonsClientAccessor accessor = mock(MLCommonsClientAccessor.class);
        TextEmbeddingProcessorFactory textEmbeddingProcessorFactory = new TextEmbeddingProcessorFactory(
            accessor,
            environment,
            clusterService
        );

        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, ImmutableMap.of("key1", "key1Mapped", "key2", "key2Mapped"));
        TextEmbeddingProcessor processor = textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
        doThrow(new RuntimeException()).when(accessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_withListTypeInput_successful() {
        List<String> list1 = ImmutableList.of("test1", "test2", "test3");
        List<String> list2 = ImmutableList.of("test4", "test5", "test6");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", list1);
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());
    }

    public void testExecute_SimpleTypeWithEmptyStringValue_throwIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "    ");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasEmptyStringValue_throwIllegalArgumentException() {
        List<String> list1 = ImmutableList.of("", "test2", "test3");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", list1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNonStringValue_throwIllegalArgumentException() {
        List<Integer> list2 = ImmutableList.of(1, 2, 3);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key2", list2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_listHasNull_throwIllegalArgumentException() {
        List<String> list = new ArrayList<>();
        list.add("hello");
        list.add(null);
        list.add("world");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key2", list);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_withMapTypeInput_successful() {
        Map<String, String> map1 = new HashMap<>();
        map1.put("test1", "test2");
        Map<String, String> map2 = new HashMap<>();
        map2.put("test3", "test4");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();

        List<List<Float>> modelTensorList = createMockVectorResult();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(any(IngestDocument.class), isNull());

    }

    public void testExecute_mapHasNonStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, Double> map2 = ImmutableMap.of("test3", 209.3D);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapHasEmptyStringValue_throwIllegalArgumentException() {
        Map<String, String> map1 = ImmutableMap.of("test1", "test2");
        Map<String, String> map2 = ImmutableMap.of("test3", "   ");
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", map1);
        sourceAndMetadata.put("key2", map2);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_mapDepthReachLimit_throwIllegalArgumentException() {
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> 1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "hello world");
        sourceAndMetadata.put("key2", ret);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testExecute_MLClientAccessorThrowFail_handlerFailure() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", "value1");
        sourceAndMetadata.put("key2", "value2");
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    private Map<String, Object> createMaxDepthLimitExceedMap(Supplier<Integer> maxDepthSupplier) {
        int maxDepth = maxDepthSupplier.get();
        if (maxDepth > 21) {
            return null;
        }
        Map<String, Object> innerMap = new HashMap<>();
        Map<String, Object> ret = createMaxDepthLimitExceedMap(() -> maxDepth + 1);
        if (ret == null) return innerMap;
        innerMap.put("hello", ret);
        return innerMap;
    }

    public void testExecute_hybridTypeInput_successful() throws Exception {
        List<String> list1 = ImmutableList.of("test1", "test2");
        Map<String, List<String>> map1 = ImmutableMap.of("test3", list1);
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put("key2", map1);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithLevel2MapConfig();
        IngestDocument document = processor.execute(ingestDocument);
        assert document.getSourceAndMetadata().containsKey("key2");
    }

    public void testExecute_simpleTypeInputWithNonStringValue_handleIllegalArgumentException() {
        Map<String, Object> sourceAndMetadata = new HashMap<>();
        sourceAndMetadata.put(IndexFieldMapper.NAME, "my_index");
        sourceAndMetadata.put("key1", 100);
        sourceAndMetadata.put("key2", 100.232D);
        IngestDocument ingestDocument = new IngestDocument(sourceAndMetadata, new HashMap<>());
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new IllegalArgumentException("illegal argument"));
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        BiConsumer handler = mock(BiConsumer.class);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        processor.execute(ingestDocument, handler);
        verify(handler).accept(isNull(), any(IllegalArgumentException.class));
    }

    public void testGetType_successful() {
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        assert processor.getType().equals(TextEmbeddingProcessor.TYPE);
    }

    public void testProcessResponse_successful() throws Exception {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);

        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
    }

    @SneakyThrows
    public void testBuildVectorOutput_withPlainStringValue_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);

        Map<String, Object> knnMap = processor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);

        // To assert the order is not changed between config map and generated map.
        List<Object> configValueList = new LinkedList<>(config.values());
        List<String> knnKeyList = new LinkedList<>(knnMap.keySet());
        assertEquals(configValueList.size(), knnKeyList.size());
        assertEquals(knnKeyList.get(0), configValueList.get(0).toString());
        int lastIndex = knnKeyList.size() - 1;
        assertEquals(knnKeyList.get(lastIndex), configValueList.get(lastIndex).toString());

        List<List<Float>> modelTensorList = createMockVectorResult();
        Map<String, Object> result = processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        assertTrue(result.containsKey("oriKey1_knn"));
        assertTrue(result.containsKey("oriKey2_knn"));
        assertTrue(result.containsKey("oriKey3_knn"));
        assertTrue(result.containsKey("oriKey4_knn"));
        assertTrue(result.containsKey("oriKey5_knn"));
        assertTrue(result.containsKey("oriKey6_knn"));
    }

    @SneakyThrows
    @SuppressWarnings("unchecked")
    public void testBuildVectorOutput_withNestedMap_successful() {
        Map<String, Object> config = createNestedMapConfiguration();
        IngestDocument ingestDocument = createNestedMapIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        Map<String, Object> favoritesMap = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("favorites");
        assertNotNull(favoritesMap);
        Map<String, Object> favoriteGames = (Map<String, Object>) favoritesMap.get("favorite.games");
        assertNotNull(favoriteGames);
        Map<String, Object> adventure = (Map<String, Object>) favoriteGames.get("adventure");
        Object actionGamesKnn = adventure.get("with.action.knn");
        assertNotNull(actionGamesKnn);
    }

    public void testBuildVectorOutput_withNestedList_successful() {
        Map<String, Object> config = createNestedListConfiguration();
        IngestDocument ingestDocument = createNestedListIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) ingestDocument.getSourceAndMetadata().get("nestedField");
        assertTrue(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(0).get("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    public void testBuildVectorOutput_withNestedList_Level2_successful() {
        Map<String, Object> config = createNestedList2LevelConfiguration();
        IngestDocument ingestDocument = create2LevelNestedListIngestDocument();
        TextEmbeddingProcessor textEmbeddingProcessor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = textEmbeddingProcessor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        textEmbeddingProcessor.buildNLPResult(knnMap, modelTensorList, ingestDocument.getSourceAndMetadata());
        Map<String, Object> nestedLevel1 = (Map<String, Object>) ingestDocument.getSourceAndMetadata().get("nestedField");
        List<Map<String, Object>> nestedObj = (List<Map<String, Object>>) nestedLevel1.get("nestedField");
        assertTrue(nestedObj.get(0).containsKey("vectorField"));
        assertTrue(nestedObj.get(1).containsKey("vectorField"));
        assertNotNull(nestedObj.get(0).get("vectorField"));
        assertNotNull(nestedObj.get(1).get("vectorField"));
    }

    public void test_updateDocument_appendVectorFieldsToDocument_successful() {
        Map<String, Object> config = createPlainStringConfiguration();
        IngestDocument ingestDocument = createPlainIngestDocument();
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        Map<String, Object> knnMap = processor.buildMapWithTargetKeyAndOriginalValue(ingestDocument);
        List<List<Float>> modelTensorList = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList);

        List<List<Float>> modelTensorList1 = createMockVectorResult();
        processor.setVectorFieldsToDocument(ingestDocument, knnMap, modelTensorList1);
        assertEquals(12, ingestDocument.getSourceAndMetadata().size());
        assertEquals(2, ((List<?>) ingestDocument.getSourceAndMetadata().get("oriKey6_knn")).size());
    }

    public void test_doublyNestedList_withMapType_successful() {
        Map<String, Object> config = createNestedListConfiguration();

        Map<String, Object> toEmbeddings = new HashMap<>();
        toEmbeddings.put("textField", "text to embedding");
        List<Map<String, Object>> l1List = new ArrayList<>();
        l1List.add(toEmbeddings);
        List<List<Map<String, Object>>> l2List = new ArrayList<>();
        l2List.add(l1List);
        Map<String, Object> document = new HashMap<>();
        document.put("nestedField", l2List);
        document.put(IndexFieldMapper.NAME, "my_index");

        IngestDocument ingestDocument = new IngestDocument(document, new HashMap<>());
        TextEmbeddingProcessor processor = createInstanceWithNestedMapConfiguration(config);
        BiConsumer handler = mock(BiConsumer.class);
        processor.execute(ingestDocument, handler);
        ArgumentCaptor<IllegalArgumentException> argumentCaptor = ArgumentCaptor.forClass(IllegalArgumentException.class);
        verify(handler).accept(isNull(), argumentCaptor.capture());
        assertEquals("list type field [nestedField] is nested list type, cannot process it", argumentCaptor.getValue().getMessage());
    }

    public void test_batchExecute_successful() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();

        List<List<Float>> modelTensorList = createMockVectorWithLength(10);
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onResponse(modelTensorList);
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNull(resultCallback.getValue().get(i).getException());
        }
    }

    public void test_batchExecute_exception() {
        final int docCount = 5;
        List<IngestDocumentWrapper> ingestDocumentWrappers = createIngestDocumentWrappers(docCount);
        TextEmbeddingProcessor processor = createInstanceWithLevel1MapConfig();
        doAnswer(invocation -> {
            ActionListener<List<List<Float>>> listener = invocation.getArgument(2);
            listener.onFailure(new RuntimeException());
            return null;
        }).when(mlCommonsClientAccessor).inferenceSentences(anyString(), anyList(), isA(ActionListener.class));

        Consumer resultHandler = mock(Consumer.class);
        processor.batchExecute(ingestDocumentWrappers, resultHandler);
        ArgumentCaptor<List<IngestDocumentWrapper>> resultCallback = ArgumentCaptor.forClass(List.class);
        verify(resultHandler).accept(resultCallback.capture());
        assertEquals(docCount, resultCallback.getValue().size());
        for (int i = 0; i < docCount; ++i) {
            assertEquals(ingestDocumentWrappers.get(i).getIngestDocument(), resultCallback.getValue().get(i).getIngestDocument());
            assertNotNull(resultCallback.getValue().get(i).getException());
        }
    }

    @SneakyThrows
    private TextEmbeddingProcessor createInstanceWithNestedMapConfiguration(Map<String, Object> fieldMap) {
        Map<String, Processor.Factory> registry = new HashMap<>();
        Map<String, Object> config = new HashMap<>();
        config.put(TextEmbeddingProcessor.MODEL_ID_FIELD, "mockModelId");
        config.put(TextEmbeddingProcessor.FIELD_MAP_FIELD, fieldMap);
        return textEmbeddingProcessorFactory.create(registry, PROCESSOR_TAG, DESCRIPTION, config);
    }

    private Map<String, Object> createPlainStringConfiguration() {
        Map<String, Object> config = new HashMap<>();
        config.put("oriKey1", "oriKey1_knn");
        config.put("oriKey2", "oriKey2_knn");
        config.put("oriKey3", "oriKey3_knn");
        config.put("oriKey4", "oriKey4_knn");
        config.put("oriKey5", "oriKey5_knn");
        config.put("oriKey6", "oriKey6_knn");
        return config;
    }

    private Map<String, Object> createNestedMapConfiguration() {
        Map<String, Object> adventureGames = new HashMap<>();
        adventureGames.put("with.action", "with.action.knn");
        adventureGames.put("with.reaction", "with.reaction.knn");
        Map<String, Object> puzzleGames = new HashMap<>();
        puzzleGames.put("maze", "maze.knn");
        puzzleGames.put("card", "card.knn");
        Map<String, Object> favoriteGames = new HashMap<>();
        favoriteGames.put("adventure", adventureGames);
        favoriteGames.put("puzzle", puzzleGames);
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite.movie", "favorite.movie.knn");
        favorite.put("favorite.games", favoriteGames);
        favorite.put("favorite.songs", "favorite.songs.knn");
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return result;
    }

    private IngestDocument createPlainIngestDocument() {
        Map<String, Object> result = new HashMap<>();
        result.put("oriKey1", "oriValue1");
        result.put("oriKey2", "oriValue2");
        result.put("oriKey3", "oriValue3");
        result.put("oriKey4", "oriValue4");
        result.put("oriKey5", "oriValue5");
        result.put("oriKey6", ImmutableList.of("oriValue6", "oriValue7"));
        return new IngestDocument(result, new HashMap<>());
    }

    private IngestDocument createNestedMapIngestDocument() {
        Map<String, Object> adventureGames = new HashMap<>();
        List<String> actionGames = new ArrayList<>();
        actionGames.add("jojo world");
        actionGames.add(null);
        adventureGames.put("with.action", actionGames);
        adventureGames.put("with.reaction", "overwatch");
        Map<String, Object> puzzleGames = new HashMap<>();
        puzzleGames.put("maze", "zelda");
        puzzleGames.put("card", "hearthstone");
        Map<String, Object> favoriteGames = new HashMap<>();
        favoriteGames.put("adventure", adventureGames);
        favoriteGames.put("puzzle", puzzleGames);
        Map<String, Object> favorite = new HashMap<>();
        favorite.put("favorite.movie", "favorite.movie.knn");
        favorite.put("favorite.games", favoriteGames);
        favorite.put("favorite.songs", "In The Name Of Father");
        Map<String, Object> result = new HashMap<>();
        result.put("favorites", favorite);
        return new IngestDocument(result, new HashMap<>());
    }

    private Map<String, Object> createNestedListConfiguration() {
        Map<String, Object> nestedConfig = new HashMap<>();
        nestedConfig.put("textField", "vectorField");
        Map<String, Object> result = new HashMap<>();
        result.put("nestedField", nestedConfig);
        return result;
    }

    private Map<String, Object> createNestedList2LevelConfiguration() {
        Map<String, Object> nestedConfig = new HashMap<>();
        nestedConfig.put("textField", "vectorField");
        Map<String, Object> nestConfigLevel1 = new HashMap<>();
        nestConfigLevel1.put("nestedField", nestedConfig);
        Map<String, Object> result = new HashMap<>();
        result.put("nestedField", nestConfigLevel1);
        return result;
    }

    private IngestDocument createNestedListIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textField", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        return new IngestDocument(nestedList, new HashMap<>());
    }

    private IngestDocument create2LevelNestedListIngestDocument() {
        HashMap<String, Object> nestedObj1 = new HashMap<>();
        nestedObj1.put("textField", "This is a text field");
        HashMap<String, Object> nestedObj2 = new HashMap<>();
        nestedObj2.put("textField", "This is another text field");
        HashMap<String, Object> nestedList = new HashMap<>();
        nestedList.put("nestedField", Arrays.asList(nestedObj1, nestedObj2));
        HashMap<String, Object> nestedList1 = new HashMap<>();
        nestedList1.put("nestedField", nestedList);
        return new IngestDocument(nestedList1, new HashMap<>());
    }
}
