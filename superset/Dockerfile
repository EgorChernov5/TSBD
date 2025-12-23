FROM apache/superset:latest

# Install necessary dependencies
USER root
RUN apt-get update && apt-get install -y \
    build-essential \
    libssl-dev libffi-dev \
    libxml2-dev libxslt1-dev \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN . /app/.venv/bin/activate && \
    uv pip install \
    # install psycopg2 for using PostgreSQL metadata store - could be a MySQL package if using that backend:
    psycopg2-binary \
    # add the driver(s) for your data warehouse(s), in this example we're showing for Microsoft SQL Server:
    pymssql \
    # package needed for using single-sign on authentication:
    Authlib \
    # openpyxl to be able to upload Excel files
    openpyxl \
    # Pillow for Alerts & Reports to generate PDFs of dashboards
    Pillow

# Copy the initialization script
COPY init_superset.sh /app/init_superset.sh

# Copy the Superset config file
COPY superset_config.py /app/pythonpath/superset_config.py

# Copy bootstrap scripts into the container
COPY bootstrap /app/bootstrap

# Ensure the script is executable
RUN chmod +x /app/init_superset.sh

# Switch back to the superset user
USER superset

# Expose the port that superset runs on
EXPOSE 8088

# Command to run the superset server
CMD ["/app/init_superset.sh"]