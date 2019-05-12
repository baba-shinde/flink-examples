package com.bss.training.flink.dto;

import java.util.Objects;

public class MovieWithSingleGenre {
    private Integer movieId;
    private String title;
    private String genre;

    public Integer getMovieId() {
        return movieId;
    }

    public void setMovieId(Integer movieId) {
        this.movieId = movieId;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getGenre() {
        return genre;
    }

    public void setGenre(String genre) {
        this.genre = genre;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieWithSingleGenre that = (MovieWithSingleGenre) o;
        return Objects.equals(movieId, that.movieId) &&
                Objects.equals(title, that.title) &&
                Objects.equals(genre, that.genre);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, title, genre);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MovieWithSingleGenre{");
        sb.append("movieId=").append(movieId);
        sb.append(", title='").append(title).append('\'');
        sb.append(", genre='").append(genre).append('\'');
        sb.append('}');
        return sb.toString();
    }
}