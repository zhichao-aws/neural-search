/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import lombok.SneakyThrows;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.util.BytesRef;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

public class HFModelAnalyzerTests extends OpenSearchTestCase {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DJLUtils.buildDJLCachePath(DJLUtilsTests.tmpDir);
    }

    @SneakyThrows
    public void testDefaultAnalyzer() {
        HFModelAnalyzer analyzer = new HFModelAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("", "hello world");
        tokenStream.reset();
        CharTermAttribute termAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
        PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);

        assertTrue(tokenStream.incrementToken());
        assertEquals("hello", termAttribute.toString());
        BytesRef payload = payloadAttribute.getPayload();
        assertNotNull(payload);
        assertEquals(6.93775f, HFModelTokenizer.bytesToFloat(payload.bytes), 0.0001f);
        assertEquals(0, offsetAttribute.startOffset());
        assertEquals(5, offsetAttribute.endOffset());

        assertTrue(tokenStream.incrementToken());
        assertEquals("world", termAttribute.toString());
        payload = payloadAttribute.getPayload();
        assertNotNull(payload);
        assertEquals(3.42089f, HFModelTokenizer.bytesToFloat(payload.bytes), 0.0001f);
        assertEquals(6, offsetAttribute.startOffset());
        assertEquals(11, offsetAttribute.endOffset());

        assertFalse(tokenStream.incrementToken());
    }

    @SneakyThrows
    public void testCustomAnalyzer() {
        HuggingFaceTokenizer tokenizer = DJLUtils.buildHuggingFaceTokenizer(
            "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-distill"
        );
        HFModelAnalyzer analyzer = new HFModelAnalyzer(() -> new HFModelTokenizer(tokenizer));
        TokenStream tokenStream = analyzer.tokenStream("", "hello world");
        tokenStream.reset();
        CharTermAttribute termAttribute = tokenStream.addAttribute(CharTermAttribute.class);
        OffsetAttribute offsetAttribute = tokenStream.addAttribute(OffsetAttribute.class);
        PayloadAttribute payloadAttribute = tokenStream.addAttribute(PayloadAttribute.class);

        assertTrue(tokenStream.incrementToken());
        assertEquals("hello", termAttribute.toString());
        BytesRef payload = payloadAttribute.getPayload();
        assertNull(payload);
        assertEquals(0, offsetAttribute.startOffset());
        assertEquals(5, offsetAttribute.endOffset());

        assertTrue(tokenStream.incrementToken());
        assertEquals("world", termAttribute.toString());
        payload = payloadAttribute.getPayload();
        assertNull(payload);
        assertEquals(6, offsetAttribute.startOffset());
        assertEquals(11, offsetAttribute.endOffset());

        assertFalse(tokenStream.incrementToken());
    }
}
