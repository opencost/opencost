FROM node:18.3.0 as builder
ADD package*.json /opt/ui/
WORKDIR /opt/ui
RUN npm install
ADD src /opt/ui/src
RUN npx parcel build src/index.html

FROM nginx:alpine
COPY --from=builder /opt/ui/dist /var/www
COPY default.nginx.conf /etc/nginx/conf.d/
COPY nginx.conf /etc/nginx/
RUN rm -rf /etc/nginx/conf.d/default.conf

RUN adduser 1001 -g 1000 -D
RUN chown 1001:1000 -R /var/www
RUN chown 1001:1000 -R /etc/nginx

ENV BASE_URL=/model


USER 1001

COPY ./docker-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
CMD ["nginx", "-g", "daemon off;"]