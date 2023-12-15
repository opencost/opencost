#!/bin/sh
set -e

if [[ ! -z "$BASE_URL_OVERRIDE" ]]; then
    echo "running with BASE_URL=${BASE_URL_OVERRIDE}"
    sed -i "s^{PLACEHOLDER_BASE_URL}^$BASE_URL_OVERRIDE^g" /var/www/*.js
else
    echo "running with BASE_URL=${BASE_URL}"
    sed -i "s^{PLACEHOLDER_BASE_URL}^$BASE_URL^g" /var/www/*.js
fi

envsubst '$API_PORT $API_SERVER $UI_PORT' < /etc/nginx/conf.d/default.nginx.conf.template > /etc/nginx/conf.d/default.nginx.conf

echo "Starting ui version $VERSION ($HEAD)"

# Run the parent (nginx) container's entrypoint script
exec /docker-entrypoint.sh "$@"
