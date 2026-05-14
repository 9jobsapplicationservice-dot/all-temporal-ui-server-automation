FROM temporalio/temporal:1.5.1 AS temporal-cli

FROM node:20-bookworm

ENV NEXT_TELEMETRY_DISABLED=1 \
    NODE_ENV=production \
    PIPELINE_WORKSPACE_ROOT=/app \
    PIPELINE_ROOT=/app/data/pipeline \
    PIPELINE_PYTHON=python3 \
    PIPELINE_TEMPORAL_AUTO_START=true \
    DISPLAY=:99 \
    PORT=3000

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      chromium \
      chromium-driver \
      fonts-liberation \
      libasound2 \
      libatk-bridge2.0-0 \
      libatk1.0-0 \
      libcups2 \
      libdbus-1-3 \
      libdrm2 \
      libgbm1 \
      libgtk-3-0 \
      libnss3 \
      libu2f-udev \
      libxcomposite1 \
      libxdamage1 \
      libxfixes3 \
      libxkbcommon0 \
      libxrandr2 \
      python3 \
      python3-pip \
      python3-venv \
      xvfb \
    && rm -rf /var/lib/apt/lists/*

COPY --from=temporal-cli /usr/local/bin/temporal /usr/local/bin/temporal

COPY requirements.txt /app/requirements.txt
RUN python3 -m pip install --break-system-packages --no-cache-dir -r /app/requirements.txt

COPY . /app

WORKDIR /app/sendemailwith-code/email-automation-nodejs
RUN npm ci --include=dev \
    && npm run build \
    && npm prune --omit=dev

WORKDIR /app
RUN chmod +x /app/scripts/start-saas.sh \
    && mkdir -p /app/data/pipeline /app/data/chrome /tmp/chrome-profile \
    && ln -sf /usr/bin/chromium /usr/bin/google-chrome

EXPOSE 3000

CMD ["/app/scripts/start-saas.sh"]
