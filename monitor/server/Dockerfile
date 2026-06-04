# === STAGE 1: BUILDER (For installing uv and resolving/installing dependencies) ===
FROM python:3.14-slim as builder

# 1. Install uv as a standalone binary (prevents 'root' user pip warnings)
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

USER root
WORKDIR /app

# 3. Use uv to create the production environment
RUN uv venv .venv && \
    . .venv/bin/activate && \
    uv pip install --no-cache-dir --python .venv/bin/python webpie pythreader jinja2 legacy-cgi

# 4. Prepare app environment as needed
ADD start.sh /app/start.sh
RUN mkdir app samples


# === STAGE 2: RUNNER (The minimal, clean production image) ===
FROM python:3.14-slim as runner

# 1. Setup environment
WORKDIR /app
ENV PATH="/app/.venv/bin:$PATH"

#### 2. Interactive mode only (to be removed for the production image): ######
##    Install vim for quick edits directly on the image (for shell-mode only debugging)
#RUN apt-get update && \
#    mkdir -p /usr/share/man/man1 && \
#    apt-get install -y -qq --no-install-recommends vim-tiny && \
#    rm -rf /var/lib/apt/lists/*
#
##.. test data from /var/cache/consistency-dump - usually not needed
#ADD reports /reports
####( END of section to be removed for production image )###### 

# 3. Copy the *clean virtual environment* and application files from the builder stage
#    This drastically reduces the size of the final image by excluding build tools and cache
COPY --from=builder app /app/
ADD app ./app

# 4. Final setup
EXPOSE 8400
RUN mkdir -p /var/cache/consistency-dump /var/cache/consistency-temp /reports/unmerged \
    && chmod +x *.sh

# 5. Define how the container starts - enable EITHER (a) OR (b)
# a) For production mode: start app right away
ENTRYPOINT ["/app/start.sh"]

# b) For interactive / testing mode: comment out the ENTRYPOINT line above
#CMD [ "/bin/bash", "--login" ]
