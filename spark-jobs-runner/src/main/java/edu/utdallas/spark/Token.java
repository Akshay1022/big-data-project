package edu.utdallas.spark;

import java.util.ArrayList;
import java.util.List;

/**
 *  Data structure that Text Analyzer uses to organize data returned by Stanford POS Tagger.
 *  It is structured as a composite patter: each token can contain other tokens.
 *  toString() outputs the parent token's string concatenated with each child string in order.
 *
 *  Object methods like token.word or token.posTag return those values for the first element.
 */
public class Token {

    private String word;
    private String posTag;
    private String nerTag;
    private String lemma;

    private Integer sentenceId;
    private Integer wordId;

    private Integer occurences;

    List<Token> components = new ArrayList<>();

    @Override
    public String toString(){
        String s = "";

        if (components.size() == 1){
            s = components.get(0).getLemma();
        }
        else {

            for (int i = 0; i < components.size(); i++) {
                s += components.get(i).getWord();
                if (i != components.size() - 1) {
                    s += " ";
                }
            }
        }

        return s.toLowerCase();
    }

    public void appendComponent(Token w){
        components.add(w);
    }

    public Boolean partialMatch(Token other){
        for (Token otherComponent : other.components){
            for (Token thisComponent : components){
                if (otherComponent.getWord().toLowerCase().compareTo(thisComponent.getWord().toLowerCase()) == 0){
                    return true;
                }
            }
        }
        return false;
    }

    public Boolean totalMatch(Token other) {
        if (other.components.size() != this.components.size()){
            return false;
        }
        else {
            for (int i = 0; i < other.components.size(); i++){
                String otherWord = other.components.get(i).getWord().toLowerCase();
                String thisWord = this.components.get(i).getWord().toLowerCase();

                if (otherWord.compareTo(thisWord) != 0){
                    return false;
                }
            }
        }

        return true;
    }

    public Token(){
        components.add(this);
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public String getPosTag() {
        return posTag;
    }

    public void setPosTag(String posTag) {
        this.posTag = posTag;
    }

    public String getNerTag() {
        return nerTag;
    }

    public void setNerTag(String nerTag) {
        this.nerTag = nerTag;
    }

    public String getLemma() {
        return lemma;
    }

    public void setLemma(String lemma) {
        this.lemma = lemma;
    }

    public Integer getSentenceId() {
        return sentenceId;
    }

    public void setSentenceId(Integer sentenceId) {
        this.sentenceId = sentenceId;
    }

    public Integer getWordId() {
        return wordId;
    }

    public void setWordId(Integer wordId) {
        this.wordId = wordId;
    }

    public boolean isCapital(){
        return word.charAt(0) == word.toUpperCase().charAt(0);
    }

    public Integer getOccurences() {
        return occurences;
    }

    public void setOccurences(Integer occurences) {
        this.occurences = occurences;
    }
}
