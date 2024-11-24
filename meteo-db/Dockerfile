FROM postgres:latest

# Install PostGIS dependencies
RUN apt-get update \
    && apt-get install -y \
    postgis \
    postgresql-15-postgis-3 \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables
ENV POSTGRES_USER=meteo
ENV POSTGRES_PASSWORD=meteo
ENV POSTGRES_DB=meteo

EXPOSE 5432
