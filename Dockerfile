FROM alpine:3.3

WORKDIR /usr/src/app

COPY dag_movva.py .

COPY driver.py .
#RUN apk add --no-cache nginx
#RUN apk update
#
#RUN apk add --update sudo
#
#RUN sudo apt update
#
#RUN apk add --update postgresql postgresql-contrib 