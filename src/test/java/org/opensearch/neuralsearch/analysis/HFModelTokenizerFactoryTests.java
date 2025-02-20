/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import java.io.StringReader;

import lombok.SneakyThrows;

import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.junit.Before;
import org.opensearch.test.OpenSearchTestCase;

public class HFModelTokenizerFactoryTests extends OpenSearchTestCase {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DJLUtils.buildDJLCachePath(DJLUtilsTests.tmpDir);
    }

    @SneakyThrows
    public void testCreateDefault() {
        Tokenizer tokenizer = HFModelTokenizerFactory.createDefault();
        assertNotNull(tokenizer);
        assertTrue(tokenizer instanceof HFModelTokenizer);
        tokenizer.setReader(new StringReader("test"));
        tokenizer.reset();
        CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);
        PayloadAttribute payloadAttribute = tokenizer.addAttribute(PayloadAttribute.class);

        assertTrue(tokenizer.incrementToken());
        assertEquals("test", charTermAttribute.toString());
        // byte ref for the token weight of test
        assertEquals("[40 86 84 c7]", payloadAttribute.getPayload().toString());
        assertFalse(tokenizer.incrementToken());
    }
}
