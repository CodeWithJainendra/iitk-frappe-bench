# Dockerfile for Frappe + iitdata
FROM frappe/bench:latest

# Install dependencies if any (none in this case, but good practice)
# RUN sudo apt-get update && sudo apt-get install -y ...

# Create a new bench
RUN bench init --skip-redis-config-generation --frappe-branch version-15 --python python3.11 frappe-bench

WORKDIR /home/frappe/frappe-bench

# Copy your custom app into the image
# Note: We assume iitdata is in the same directory as the Dockerfile during build
COPY ./apps/iitdata ./apps/iitdata

# Install the app
RUN ./env/bin/pip install -e ./apps/iitdata

# Expose ports
EXPOSE 8000 9000 6379 3306
