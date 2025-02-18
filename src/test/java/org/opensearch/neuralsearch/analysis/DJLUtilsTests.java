/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;

import lombok.SneakyThrows;

import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;

public class DJLUtilsTests extends OpenSearchTestCase {
    public static Path tmpDir = Path.of(".", "data");

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DJLUtils.buildDJLCachePath(tmpDir);
    }

    public void testBuildDJLCachePath() {
        assertEquals("./data/ml_cache", DJLUtils.ML_CACHE_PATH.toString());
    }

    public void testIsValidTokenizerId() {
        // Test valid tokenizer IDs
        assertTrue(DJLUtils.isValidTokenizerId("opensearch-project/valid-tokenizer"));
        assertTrue(DJLUtils.isValidTokenizerId("opensearch-project/another-valid-tokenizer"));

        // Test invalid tokenizer IDs
        assertFalse(DJLUtils.isValidTokenizerId(""));
        assertFalse(DJLUtils.isValidTokenizerId(null));
        assertFalse(DJLUtils.isValidTokenizerId("invalid-tokenizer"));
        assertFalse(DJLUtils.isValidTokenizerId("opensearch-project/invalid/tokenizer"));
        assertFalse(DJLUtils.isValidTokenizerId("other-project/tokenizer"));
    }

    public void testBuildHuggingFaceTokenizer_InvalidTokenizerId() {
        Exception exception = assertThrows(IllegalArgumentException.class, () -> DJLUtils.buildHuggingFaceTokenizer("invalid-tokenizer"));
        assertEquals("tokenizer id [invalid-tokenizer] is not allowed.", exception.getMessage());

        Exception exception2 = assertThrows(
            RuntimeException.class,
            () -> DJLUtils.buildHuggingFaceTokenizer("opensearch-project/invalid-tokenizer")
        );
        assertEquals(
            "request error: https://huggingface.co/opensearch-project/invalid-tokenizer/resolve/main/tokenizer.json: status code 401",
            exception2.getMessage()
        );
    }

    public void testBuildHuggingFaceTokenizer_thenSuccess() {
        HuggingFaceTokenizer tokenizer = DJLUtils.buildHuggingFaceTokenizer(
            "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill"
        );
        assertNotNull(tokenizer);
        assertEquals(512, tokenizer.getMaxLength());
        Encoding result = tokenizer.encode("hello world");
        assertEquals(4, result.getIds().length);
        assertEquals(7592, result.getIds()[1]);
    }

    @SneakyThrows
    public void testParseInputStreamToTokenWeights_InvalidFormat() {
        String content = "token1\t0.5\ntoken2\n";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
        Exception exception = assertThrows(IllegalArgumentException.class, () -> DJLUtils.parseInputStreamToTokenWeights(inputStream));
        assertEquals("Invalid line in token weights file: token2", exception.getMessage());

        InputStream inputStream2 = new ByteArrayInputStream("test\tdata".getBytes());
        Exception exception2 = assertThrows(NumberFormatException.class, () -> DJLUtils.parseInputStreamToTokenWeights(inputStream2));
        assertEquals("For input string: \"data\"", exception2.getMessage());
    }

    public void testParseInputStreamToTokenWeights_thenSuccess() {
        String content = "token1\t0.5\ntoken2\t0.2\n";
        InputStream inputStream = new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));

        Map<String, Float> tokenWeights = DJLUtils.parseInputStreamToTokenWeights(inputStream);
        assertEquals(2, tokenWeights.size());
        assertEquals(0.5f, tokenWeights.get("token1"), 0.0f);
        assertEquals(0.2f, tokenWeights.get("token2"), 0.0f);
    }

    public void testFetchTokenWeights_InvalidParams() {
        Exception exception = assertThrows(
            IllegalArgumentException.class,
            () -> DJLUtils.fetchTokenWeights("invalid-tokenizer", "invalid-file")
        );
        assertEquals("tokenizer id [invalid-tokenizer] is not allowed.", exception.getMessage());

        Exception exception2 = assertThrows(
            RuntimeException.class,
            () -> DJLUtils.fetchTokenWeights("opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill", "invalid-file")
        );
        assertEquals(
            "Failed to download file from https://huggingface.co/opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill/resolve/main/invalid-file",
            exception2.getMessage()
        );
    }

    public void testFetchTokenWeights_thenSuccess() {
        Map<String, Float> tokenWeights = DJLUtils.fetchTokenWeights(
            "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill",
            "query_token_weights.txt"
        );
        assertNotNull(tokenWeights);
        assertEquals(6.93775f, tokenWeights.get("hello"), 0.0001f);
    }
}
