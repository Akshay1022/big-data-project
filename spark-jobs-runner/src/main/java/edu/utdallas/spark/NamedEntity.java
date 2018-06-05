package edu.utdallas.spark;

/**
 *  Data structure that represents the database table NAMEDENTITY rows.
 */
public class NamedEntity extends Word {

    private Integer id;
    private String name;
    private Integer weight;
    private Boolean isVip;

    @Override
    public String toString(){
        return name + " - " + weight;
    }

    @Override
    public String getWord() {
        return name.toLowerCase();
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getWeight() {
        return weight;
    }

    public void setWeight(Integer weight) {
        this.weight = weight;
    }

    public Boolean getVip() {
        return isVip;
    }

    public void setVip(Boolean vip) {
        isVip = vip;
    }
}
