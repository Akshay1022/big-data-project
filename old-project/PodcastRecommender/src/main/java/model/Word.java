package model;

import analyzer.Token;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Data structure that provides polymorphic capabilities to Keyword and NamedEntity data structures.
 */
public class Word {

    private Integer id;
    private Integer weight;
    private Integer df;
    private Double idf;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public Integer getDf() {
        return df;
    }

    public void setDf(Integer df) {
        this.df = df;
    }

    public Double getIdf() {
        return idf;
    }

    public void setIdf(Double idf) {
        this.idf = idf;
    }
}
