# Use a Node.js 20 base image (slim variant for smaller size)
FROM node:20-slim

# Set working directory
WORKDIR /app

# Install build tools for compiling better-sqlite3, then install dependencies, then remove tools
COPY package*.json ./
RUN apt-get update && \
    apt-get install -y python3 build-essential && \ 
    npm ci --omit=dev && \ 
    apt-get remove -y build-essential python3 && \
    apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Copy the application source code
COPY . ./

# Create directory for persistent data (SQLite DB, dedup cache)
RUN mkdir -p /app/data

# Expose the webhook port (if using Helius/QuickNode webhooks)
EXPOSE 8080

# Launch the watcher
CMD ["node", "node-app.js"]
