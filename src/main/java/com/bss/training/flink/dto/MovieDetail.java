package com.bss.training.flink.dto;

import java.util.List;
import java.util.Objects;

public class MovieDetail {
    private Integer movieId;
    private String title;
    private List<String> genres;

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

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieDetail movie = (MovieDetail) o;
        return Objects.equals(movieId, movie.movieId) &&
                Objects.equals(title, movie.title) &&
                Objects.equals(genres, movie.genres);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, title, genres);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("MovieDetail{");
        sb.append("movieId=").append(movieId);
        sb.append(", title='").append(title).append('\'');
        sb.append(", genres=").append(genres);
        sb.append('}');
        return sb.toString();
    }
}
