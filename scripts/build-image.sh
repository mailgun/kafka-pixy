#!/usr/bin/env bash
TAG=$(git describe --tags | cut -c 2-)
docker build . -t mailgun/kafka-pixy:${TAG} -t mailgun/kafka-pixy:latest
