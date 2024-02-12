#!/bin/sh
set -e

cp -rv /opt/ui/dist/* /var/www

if [[ ! -z "$BASE_URL_OVERRIDE" ]]; then
    echo "running with BASE_URL=${BASE_URL_OVERRIDE}"
    sed -i "s^{PLACEHOLDER_BASE_URL}^$BASE_URL_OVERRIDE^g" /var/www/*.js
else
    echo "running with BASE_URL=${BASE_URL}"
    sed -i "s^{PLACEHOLDER_BASE_URL}^$BASE_URL^g" /var/www/*.js
fi

# export your OPENCOST_FOOTER_CONTENT='<a href="https://opencost.io">OpenCost</a>' in your Dockerfile to set
if [[ ! -z "$OPENCOST_FOOTER_CONTENT" ]]; then
    sed -i "s^PLACEHOLDER_FOOTER_CONTENT^$OPENCOST_FOOTER_CONTENT^g" /var/www/*.js
else
    sed -i "s^PLACEHOLDER_FOOTER_CONTENT^OpenCost version: $VERSION ($HEAD)^g" /var/www/*.js
fi

envsubst '$API_PORT $API_SERVER $UI_PORT' < /etc/nginx/conf.d/default.nginx.conf.template > /etc/nginx/conf.d/default.nginx.conf

echo "Starting OpenCost UI version $VERSION ($HEAD)"

# Run the parent (nginx) container's entrypoint script
exec /docker-entrypoint.sh "$@"
