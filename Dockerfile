FROM ubuntu:latest
LABEL authors="yann-y"

ENTRYPOINT ["top", "-b"]