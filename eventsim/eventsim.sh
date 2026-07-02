#!/bin/bash
java -XX:+UseG1GC -XX:+UseStringDeduplication -Xmx3G \
  -Dlogback.configurationFile=/opt/eventsim/logback.xml \
  -jar eventsim-assembly-2.0.jar $*
