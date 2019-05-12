package com.bss.training.flink.dto;

import java.util.Objects;

public class GenereConfig {
    private String genere;
    private Boolean allowed;

    public String getGenere() {
        return genere;
    }

    public void setGenere(String genere) {
        this.genere = genere;
    }

    public Boolean getAllowed() {
        return allowed;
    }

    public void setAllowed(Boolean allowed) {
        this.allowed = allowed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GenereConfig that = (GenereConfig) o;
        return Objects.equals(genere, that.genere) &&
                Objects.equals(allowed, that.allowed);
    }

    @Override
    public int hashCode() {
        return Objects.hash(genere, allowed);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("GenereConfig{");
        sb.append("genere='").append(genere).append('\'');
        sb.append(", allowed=").append(allowed);
        sb.append('}');
        return sb.toString();
    }
}
