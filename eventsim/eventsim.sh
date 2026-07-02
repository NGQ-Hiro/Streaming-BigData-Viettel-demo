#!/bin/bash
java -XX:+AggressiveOpts -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx8G \
  -Dlogback.configurationFile=/opt/eventsim/logback.xml \
  -jar eventsim-assembly-2.0.jar $*

