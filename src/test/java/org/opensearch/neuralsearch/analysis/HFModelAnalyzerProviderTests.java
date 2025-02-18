/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.analysis;

import org.apache.lucene.analysis.Analyzer;
import org.junit.Before;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.AnalyzerProvider;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

public class HFModelAnalyzerProviderTests extends OpenSearchTestCase {
    @Before
    public void setUp() throws Exception {
        super.setUp();
        DJLUtils.buildDJLCachePath(DJLUtilsTests.tmpDir);
    }

    public void testProvide() {
        final Index index = new Index("test", "_na_");
        Settings settings = Settings.builder()
            .put("tokenizer_id", "opensearch-project/opensearch-neural-sparse-encoding-doc-v2-mini")
            .build();
        final IndexSettings indexProperties = IndexSettingsModule.newIndexSettings(index, settings);

        AnalyzerProvider<HFModelAnalyzer> analyzerProvider = new HFModelAnalyzerProvider(indexProperties, null, "test", settings);
        assertNotNull(analyzerProvider);
        Analyzer analyzer = analyzerProvider.get();
        assertNotNull(analyzer);
        assertSame(analyzer, analyzerProvider.get());
    }
}
