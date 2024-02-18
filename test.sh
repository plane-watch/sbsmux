#!/usr/bin/env bash

set -x

TMPFILE=$(mktemp)

make sbsmux

./bin/sbsmux \
  --statsinterval=5 \
  --bufsizein=100000 \
  --bufsizeout=100000 \
  &
SBSMUX_PID=$!

echo "wait for sbsmux to start up"
sleep 5

nc 127.0.0.1 30003 > "$TMPFILE" &
NC_OUT_PID=$!

cat "./cmd/sbsmux/test_data/readsb.sbs.output" | pv --force --name " SBS data sent " -b -L 256000 | nc -v 127.0.0.1 30103

echo "wait for sbsmux to flush all data"
sleep 10

kill $NC_OUT_PID > /dev/null 2>&1
kill $SBSMUX_PID > /dev/null 2>&1


diff --strip-trailing-cr <(sort "./cmd/sbsmux/test_data/readsb.sbs.output") <(sort "$TMPFILE")
EXITCODE=$?

rm "$TMPFILE"

if [[ "$EXITCODE" -ne 0 ]]; then
    echo "TEST FAILS - OUTPUT DIFFERS FROM INPUT"
else
    echo "TEST SUCCEEDS - OUTPUT MATCHES INPUT"
fi

exit $EXITCODE