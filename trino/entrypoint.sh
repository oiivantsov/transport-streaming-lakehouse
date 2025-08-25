#!/bin/bash
chown -R trino:trino /data || true
exec /usr/lib/trino/bin/launcher run --etc-dir /etc/trino