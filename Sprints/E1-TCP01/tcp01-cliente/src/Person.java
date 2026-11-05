package tcp01;

import java.io.Serializable;

public class Person implements Serializable {
    private static final long serialVersionUID = 1L;

    private String name;
    private Place place;
    private int year;

    public Person(String name, Place place, int year) {
        this.name = name;
        this.place = place;
        this.year = year;
    }

    public String getName() { return name; }
    public Place getPlace() { return place; }
    public int getYear() { return year; }

    @Override
    public String toString() {
        return name + " (" + year + "), " + place;
    }
}
