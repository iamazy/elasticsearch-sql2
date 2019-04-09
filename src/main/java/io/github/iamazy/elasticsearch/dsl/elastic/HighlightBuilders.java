package io.github.iamazy.elasticsearch.dsl.elastic;

import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;


import java.util.List;

public class HighlightBuilders {

    public static HighlightBuilder highlighter(List<String> highlighter) {
        HighlightBuilder highlightBuilder = new HighlightBuilder().requireFieldMatch(false);
        for (String field : highlighter) {
            highlightBuilder.field(field, 500, 0);
        }
        highlightBuilder.preTags("<span style=\"color:red\">");
        highlightBuilder.postTags("</span>");
        return highlightBuilder;
    }

}
