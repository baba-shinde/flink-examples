package com.bss.training.flink.dto;

import java.util.Objects;
import java.util.Set;

public class Movie {
    private String name;
    private Set<String> genres;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Set<String> getGenres() {
        return genres;
    }

    public void setGenres(Set<String> genres) {
        this.genres = genres;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Movie movie = (Movie) o;
        return Objects.equals(name, movie.name) &&
                Objects.equals(genres, movie.genres);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, genres);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MovieDetail{");
        sb.append("name='").append(name).append('\'');
        sb.append(", genres=").append(genres);
        sb.append('}');
        return sb.toString();
    }
}
