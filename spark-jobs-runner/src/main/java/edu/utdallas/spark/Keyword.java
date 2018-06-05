package edu.utdallas.spark;

/**
 * Data structure that represents KEYWORD table rows.
 */
public class Keyword extends Word {

    private Integer id;
    private String word;
    private String category;
    private Integer weight;

    @Override
    public String getWord() { return word.toLowerCase(); }

    @Override
    public String toString(){
        return word + " - " + weight;
    }

    /************** GETTERS AND SETTERS ******************/

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }
}
