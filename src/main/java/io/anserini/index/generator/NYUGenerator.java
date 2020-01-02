package io.anserini.index.generator;

import io.anserini.collection.SourceDocument;
import io.anserini.index.IndexArgs;
import io.anserini.index.IndexCollection;
import io.anserini.index.transform.NYUFeaturesTransform;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;

import java.util.Map;
import java.util.function.BiConsumer;

public class NYUGenerator extends LuceneDocumentGenerator {

    public NYUGenerator()  {
        super(new NYUFeaturesTransform());
    }

    public NYUGenerator(IndexArgs args, IndexCollection.Counters counters) {
        super(new NYUFeaturesTransform(), args, counters);
    }

    @Override
    public Document createDocument(SourceDocument src) {

        Document doc = super.createDocument(src);
        if (doc == null) return null;

        // process other features
        ((NYUFeaturesTransform)super.transform).getFeatures().forEach((k,v) -> {
                if (k != null && v != null) doc.add(new StoredField(k, v));
        });
        ((NYUFeaturesTransform)super.transform).getOutlinks().forEach(s -> {
                if (s != null && s.length()>0) doc.add(new StoredField("outlinks", s));
        });

        return doc;
    }
}
