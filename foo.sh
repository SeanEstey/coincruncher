#!/bin/sh

if [ $# -ne 1 ]; then
  echo "$0 <git-commit>" 1>&2
  exit 1
fi

git diff --name-status $1
read -p "Press any key to execute..."

rm -rf ./tmp
git diff --name-only $1 | git checkout-index --prefix=./tmp/htdocs/ --stdin
git diff --name-status $1 | awk '{if($1=="D") print $2}' > ./tmp/deleted

auto_ssh()
{
    expect -c "
    spawn $1
    expect {
      \" Are you sure you want to continue connecting (yes/no)?\" {
        send \"yes\r\"
        expect \"password:\"
        send \"${2}\r\"
      }
      \"password:\" {
      send \"${2}\r\"
      }
    }
    interact
    "
}



#FILES=()
# 's/\s*[a-zA-Z?]\+ \(.*\)/\1/'
#for i in $( git status -s | sed 's/\s*[a-zA-Z?]\+ \(.*\)/\1/' ); do
#    FILES+=( "$i" )
#done
#echo "${FILES[@]}"
