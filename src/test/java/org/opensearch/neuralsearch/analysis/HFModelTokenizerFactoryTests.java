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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.test.IndexSettingsModule;
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

    public void testNewHFModelTokenizerFactory_withInvalidParams() {
        final Index index = new Index("test", "_na_");

        Settings settings1 = Settings.builder().build();
        final IndexSettings indexProperties1 = IndexSettingsModule.newIndexSettings(index, settings1);
        Exception exception1 = expectThrows(
            NullPointerException.class,
            () -> new HFModelTokenizerFactory(indexProperties1, null, "test_tokenizer", settings1)
        );
        assertTrue(exception1.getMessage().contains("tokenizer_id is required"));
    }

    @SneakyThrows
    public void testCreateCustomTokenizer() {
        final Index index = new Index("test", "_na_");
        Settings settings = Settings.builder()
            .put("tokenizer_id", "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-mini")
            .build();
        final IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, settings);

        HFModelTokenizerFactory factory = new HFModelTokenizerFactory(indexProperties, null, "test_tokenizer", settings);
        Tokenizer tokenizer = factory.create();
        assertNotNull(tokenizer);
        assertTrue(tokenizer instanceof HFModelTokenizer);

        tokenizer.setReader(new StringReader("test"));
        tokenizer.reset();
        CharTermAttribute charTermAttribute = tokenizer.addAttribute(CharTermAttribute.class);
        PayloadAttribute payloadAttribute = tokenizer.addAttribute(PayloadAttribute.class);

        assertTrue(tokenizer.incrementToken());
        assertEquals("test", charTermAttribute.toString());
        assertNull(payloadAttribute.getPayload());
        assertFalse(tokenizer.incrementToken());
    }
}
