package io.anserini.index.transform;

import com.google.common.util.concurrent.AtomicDouble;
import org.elasticsearch.common.Strings;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import javax.print.Doc;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class NYUFeaturesTransform extends StringTransform {

    protected final Map<String, String> features;
    protected final Set<String> outlinks;
    protected String contents;
    protected Document document;
    protected List<String> words;
    protected int visTerms;

    public NYUFeaturesTransform() {
        contents = null;
        document = null;
        words = null;
        features = new HashMap<>();
        outlinks = new HashSet<>();
    }

    @Override
    public String apply(String s) {

        document = Jsoup.parse(s);

        contents = document.text();
        words = Arrays.stream(strNormalize(contents).split("\\s"))
            .filter((w) -> w.length()>0)
            .collect(Collectors.toList());

        visTerms = words.size();

        Map<String, Float> pd = mle(words);

        // document features
        // see reference

        features.clear();
        features.put("numVisibleTerms_f_i",Integer.toString(visTerms));
        features.put("fracVisText_f_f", fracVisText(s));
        features.put("numTitleTerms_f_i", numTitleTerms());
        features.put("avgTermLength_f_f", avgTermLength());
        features.put("fracAnchorText_f_f", fracText("a",visTerms));
        features.put("fracTabletext_f_f", fracText("td",visTerms));
        features.put("entropy_f_f", entropy(pd));
        features.put("avgTermP_f_f", avgP(pd));
        features.put("avgBigramP_f_f", avgBigramP());

        outlinks.clear();
        document.select("a").forEach(e -> {
            try {
                String h = (new URI(e.attr("href"))).getHost();
                if (h != null && h.length()>0) outlinks.add(h);
            } catch (URISyntaxException ex) {}
        });

        return contents;
    }

    public Map<String, String> getFeatures() {
        return features;
    }

    public Set<String> getOutlinks() {
        return outlinks;
    }

    // compute P(t|C) as the MLE of each token, given a collection of tokens
    protected Map<String, Float> mle(List<String> collection) {

        // term freq
        Map<String,Integer> map = new HashMap<>();
        for (String w: collection) {
            if (w.length()>0) map.put(w, map.getOrDefault(w,0)+1);
        }

        // MLE
        Map<String, Float> mle = new HashMap<>();
        for (Map.Entry<String, Integer> e: map.entrySet()) {
            mle.put(e.getKey(), e.getValue().floatValue()/collection.size());
        }

        return mle;
    }

    // create a bigram set from the contents
    protected List<String> bigrams() {

        final List<String> b = new ArrayList<>();
        for (int i=0; i<words.size()-1; i++) {
            b.add(words.get(i) + " " + words.get(i+1));
        }
        return b;
    }

    // clean up strings
    protected String strNormalize(String s) {
        return s.toLowerCase().replaceAll("\\W"," ");
    }


    // features
    // ========

    protected String numTitleTerms() {
        return Integer.toString(document.title().split("\\s").length);
    }

    protected String avgBigramP() {
        // create a bigram list from the content; compute probabilities
        Map<String,Float> b = mle(bigrams());
        float b_sum = b.values().stream().reduce(0f, Float::sum);
        return String.valueOf(b_sum/b.size());
    }

    // average Probability of a collection<String,Float>
    protected String avgP(Map<String,Float> pd) {
        float sum = pd.values().stream().reduce(0f, Float::sum);
        return String.valueOf(sum/pd.size());
    }

    // compute entropy over doc terms
    protected String entropy(Map<String, Float> pd) {

        // entropy
        float entropy = 0;
        for (Map.Entry<String, Float> e: pd.entrySet()) {
            entropy += e.getValue() * Math.log(e.getValue());
        }
        return Float.toString(-entropy);
    }

    protected String fracText(String selector, int length) {
        StringBuffer buffer = new StringBuffer();
        document.select(selector).forEach(e -> buffer.append(e.text()).append(" "));
        float n = Arrays.stream(strNormalize(buffer.toString()).split("\\s"))
            .filter(w -> w.length()>0)
            .count();
        return Float.toString(n/length);
    }

//    protected String fracAnchorText() {
//        float n = document.select("a")
//                .stream()
//                .mapToInt(e -> strNormalize(e.text()).split("\\s").length)
//                .sum();
//        return Float.toString(n/visTerms);
//    }

    protected String avgTermLength() {
        int n = Arrays.stream(contents.split("\\s"))
            .map(w -> w.length())
            .reduce(0, Integer::sum);
        return Float.toString(((float)n)/contents.length());
    }

    protected String fracVisText(String s) {
        return Float.toString(((float)visTerms)/s.split("\\s").length);
    }

}
