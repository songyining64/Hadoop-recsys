FROM apache/spark:3.5.3-java17-python3
USER root
RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
    fonts-noto-cjk \
    fontconfig \
  && fc-cache -fv \
  && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir matplotlib pandas numpy scikit-learn
USER root
