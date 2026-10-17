FROM spark:3.5.9-scala2.12-java17-python3-ubuntu

USER root
# Match the host user while keeping a valid account for Java/Hadoop.
RUN groupmod -g 1000 spark && usermod -u 1000 -g 1000 spark
USER spark
