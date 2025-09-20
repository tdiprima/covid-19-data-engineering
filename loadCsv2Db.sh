#!/usr/bin/env bash
#
# loadCsv2Db.sh - Load CSV data into MongoDB via PathDB collection
# Wrapper script for load_tables_daily.py to import COVID-19 data
#

echo "-----------------------------------------------------"
echo "Start Date: $(date)               Host:$(hostname)   "
echo "-----------------------------------------------------"

PROGNAME=$(basename "$0")

NORMAL="\\033[0;39m"
RED="\\033[1;31m"

usage() {
  printf "${RED}USAGE:\n"
  # echo "If pathdb:"
  printf "    $PROGNAME --src [data_folder] --collectionname [pathdb collection] --user [username] --passwd [password]${NORMAL}\n"
  # echo "Else:"
  # echo "$PROGNAME dbhost dbport dbname manifest"
  exit 1
}

error_exit() {
  echo "${PROGNAME}: ${1:-"Error"}" 1>&2
  exit 1
}

# Check input
if [[ $# -eq 8 ]]; then
  # Do all the things
  python3 /app/load_tables_daily.py --dbhost "ca-mongo" --dbport 27017 --dbname camic --pathdb --url "http://quip-pathdb" "$@" || error_exit $LINENO
else
  usage
fi


echo "-----------------------------------------------------"
echo "End Date: $(date)                 Host:$(hostname)   "
echo "-----------------------------------------------------"