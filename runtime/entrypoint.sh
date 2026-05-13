#!/bin/sh
set -e
/usr/local/bin/runtime-server &
exec /usr/local/bin/runtime-agent
