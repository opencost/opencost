FROM node:12.22

ADD server /home/servr/

RUN cd /home/servr && npm install; \
    apt-get update && apt-get install nginx -y --no-install-recommends

CMD sh -c 'cd /home/servr && service nginx start && npx parcel src/index.html'