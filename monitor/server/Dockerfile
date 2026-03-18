# === STAGE 1: BUILDER (For installing uv and resolving/installing dependencies) ===
FROM python:3.11-slim as builder
#USER root
WORKDIR /app

# 1. Install uv as a standalone, fast package manager
#    We use pipx to install it outside of the main environment
ENV PATH="/root/.local/bin:$PATH"
RUN pip install --upgrade pip \
  && pip install --no-cache-dir pipx \
  && pipx install uv

# 2. install vim for quick edits directly on the image (for shell-mode only)
##RUN apt-get update && apt-get install -y --no-install-recommends vim-tiny \
##  && rm -rf /var/lib/apt/lists/*


# 3. Use uv to create the production environment and install required modules
#    'uv venv' creates the venv; 'uv pip sync' installs locked dependencies fast
RUN uv venv --system-site-packages .venv && \
    . .venv/bin/activate && \
    uv pip install webpie pythreader jinja2 \
    uv clean

# 4. copy local files as needed

ADD start.sh /app/start.sh
RUN mkdir app samples


# === STAGE 2: RUNNER (The minimal, clean production image) ===
FROM python:3.11-slim as runner

# 1. Set the working directory
WORKDIR /app

# 2. Copy the *clean virtual environment* and application files from the builder stage
#    This drastically reduces the size of the final image by excluding build tools and cache
COPY --from=builder app /app/

ADD start.sh /root/start.sh
RUN mkdir -p /var/cache/consistency-dump /var/cache/consistency-temp /reports/unmerged
ADD app ./app

###.. test data from /var/cache/consistency-dump - usually not needed
#ADD app/reports /reports

RUN chmod +x *.sh


# 3. Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"


# 4. Define how the container starts
EXPOSE 8400
ENTRYPOINT ["/root/start.sh"]


# 5. For interactive mode: comment out the ENTRYPOINT line above
#COPY --from=builder /usr/bin/vi /usr/bin/vi
CMD [ "/bin/bash", "--login" ]
