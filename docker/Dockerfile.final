FROM scratch

MAINTAINER Derek Collison <derek@apcera.com>

ADD gnatsd /gnatsd

CMD []
ENTRYPOINT ["/gnatsd", "-p", "4222", "-m", "8333"]

EXPOSE 4222 8333