/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import lombok.SneakyThrows;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class HFModelTokenizerTests extends OpenSearchTestCase {
    private HuggingFaceTokenizer huggingFaceTokenizer;
    private Map<String, Float> tokenWeights;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        DJLUtils.buildDJLCachePath(DJLUtilsTests.tmpDir);
        huggingFaceTokenizer = DJLUtils.buildHuggingFaceTokenizer("opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill");
        tokenWeights = new HashMap<>();
        tokenWeights.put("hello", 0.5f);
        tokenWeights.put("world", 0.3f);
    }

    @SneakyThrows
    public void testTokenizeWithoutWeights() {
        HFModelTokenizer tokenizer = new HFModelTokenizer(huggingFaceTokenizer);
        tokenizer.setReader(new StringReader("hello world a"));
        tokenizer.reset();

        CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
        PayloadAttribute payloadAtt = tokenizer.addAttribute(PayloadAttribute.class);

        assertTrue(tokenizer.incrementToken());
        assertEquals("hello", termAtt.toString());
        BytesRef payload = payloadAtt.getPayload();
        assertNull(payload);

        assertTrue(tokenizer.incrementToken());
        assertEquals("world", termAtt.toString());
        payload = payloadAtt.getPayload();
        assertNull(payload);

        assertTrue(tokenizer.incrementToken());
        assertEquals("a", termAtt.toString());
        payload = payloadAtt.getPayload();
        assertNull(payload);

        // No more tokens
        assertFalse(tokenizer.incrementToken());
    }

    @SneakyThrows
    public void testTokenizeWithWeights() {
        HFModelTokenizer tokenizer = new HFModelTokenizer(huggingFaceTokenizer, tokenWeights);
        tokenizer.setReader(new StringReader("hello world a"));
        tokenizer.reset();

        CharTermAttribute termAtt = tokenizer.addAttribute(CharTermAttribute.class);
        PayloadAttribute payloadAtt = tokenizer.addAttribute(PayloadAttribute.class);

        assertTrue(tokenizer.incrementToken());
        assertEquals("hello", termAtt.toString());
        BytesRef payload = payloadAtt.getPayload();
        assertNotNull(payload);
        assertEquals(0.5f, HFModelTokenizer.bytesToFloat(payload.bytes), 0.0001f);

        assertTrue(tokenizer.incrementToken());
        assertEquals("world", termAtt.toString());
        payload = payloadAtt.getPayload();
        assertNotNull(payload);
        assertEquals(0.3f, HFModelTokenizer.bytesToFloat(payload.bytes), 0.0001f);

        assertTrue(tokenizer.incrementToken());
        assertEquals("a", termAtt.toString());
        payload = payloadAtt.getPayload();
        assertNotNull(payload);
        assertEquals(1f, HFModelTokenizer.bytesToFloat(payload.bytes), 0f);

        // No more tokens
        assertFalse(tokenizer.incrementToken());
    }

    @SneakyThrows
    public void testTokenizeLongText() {
        // Create a text longer than the max length to test overflow handling
        StringBuilder longText = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            longText.append("hello world ");
        }

        HFModelTokenizer tokenizer = new HFModelTokenizer(huggingFaceTokenizer);
        tokenizer.setReader(new StringReader(longText.toString()));
        tokenizer.reset();

        int tokenCount = 0;
        while (tokenizer.incrementToken()) {
            tokenCount++;
        }

        assertEquals(2000, tokenCount);
    }

    public void testFloatBytesConversion() {
        float originalValue = 0.5f;
        byte[] bytes = HFModelTokenizer.floatToBytes(originalValue);
        float convertedValue = HFModelTokenizer.bytesToFloat(bytes);
        assertEquals(originalValue, convertedValue, 0f);
    }
}
