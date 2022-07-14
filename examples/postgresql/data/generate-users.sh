#!/usr/bin/env bash

COUNT="10"
FORMAT="json"

# parse args
while [[ $# -gt 0 ]]; do
  case $1 in
    -n|--count)
      COUNT="$2"
      shift
      shift
      ;;
    -f|--format)
      FORMAT="$2"
      shift
      shift
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

DATA_URL="https://randomuser.me/api/?format=json&results=${COUNT}&nat=us&inc=location,name,login,email,dob"
JSON_TEMPLATE="{first_name: .name.first, last_name: .name.last, city: .location.city, state: .location.state, country: .location.country, email: .email, username: .login.username, date_of_birth: .dob.date}";

to_sql () {
  FIRST="true"
  echo 'INSERT INTO users (first_name, last_name, city, "state", country, email, username, date_of_birth) VALUES'
  while read -r line
  do
    if [ "$FIRST" = false ] ; then
      echo ","
    fi
    echo "$line" | jq -rc "\"  ('\(.first_name)', '\(.last_name)', '\(.city)', '\(.state)', '\(.country)', '\(.email)', '\(.username)', '\(.date_of_birth)')\"" | tr -d '\n'
    FIRST="false"
  done
  echo ";"
}

case $FORMAT in
  json)
    curl -s "$DATA_URL" | jq -c ".results | .[] | ${JSON_TEMPLATE}"
    ;;
  sql)
    curl -s "$DATA_URL" | jq -c ".results | .[] | ${JSON_TEMPLATE}" | to_sql
    ;;
esac
