package com.iamazy.springcloud.elasticsearch.dsl.sql.utils;

/**
 * @author iamazy
 */
public interface Constants {

    String POUND="#";
    String DEFAULT_ES_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZ";
    /**
     * Full Text Query Args
     */
    String BOOLEAN="boolean";
    String PHRASE="phrase";
    String PHRASE_PREFIX="phrase_prefix";
    String NONE="none";
    String ALL="all";
    String AND="and";
    String NULL="null";
    String OR="or";
    String OPERATOR="operator";
    String MINIMUM_SHOULD_MATCH="minimum_should_match";
    String ANALYZER="analyzer";
    String BOOST="boost";
    String PREFIX_LENGTH="prefix_length";
    String MAX_EXPANSIONS="max_expansions";
    String FUZZY_TRANSPOSITIONS="fuzzy_transpositions";
    String LENIENT="lenient";
    String ZERO_TERMS_QUERY="zero_terms_query";
    String FUZZINESS="fuzziness";
    String SLOP="slop";
    String TYPE="type";
    String FUZZY_REWRITE="fuzzy_rewrite";
    String CUTOFF_FREQUENCY="cutoff_Frequency";
    String USE_DIS_MAX="use_dis_max";
    String TIE_BREAKER="tie_breaker";
    String QUOTE_ANALYZER="quote_analyzer";
    String AUTO_GENERATE_PHRASE_QUERIES="auto_generate_phrase_queries";
    String MAX_DETERMINIZED_STATES="max_determinized_states";
    String ALLOW_LEADING_WILDCARD="allow_leading_wildcard";
    String ENABLE_POSITION_INCREMENTS="enable_position_increments";
    String FUZZY_PREFIX_LENGTH="fuzzy_prefix_length";
    String FUZZY_MAX_EXPANSIONS="fuzzy_max_expansions";
    String REWRITE="rewrite";
    String PHRASE_SLOP="phrase_slop";
    String ANALYZE_WILDCARD="analyze_wildcard";
    String QUOTE_FIELD_SUFFIX="quote_field_suffix";
    String TIME_ZONE="time_zone";
    String ESCAPE="escape";
    String DEFAULT_OPERATOR="default_operator";
    String FLAGS="flags";
    String FLAGS_VALUE="flags_value";
    String FIELDS="fields";
    String DEFAULT_FIELD="default_field";
    String TRANSPOSITIONS="transpositions";
}