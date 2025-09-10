#!/bin/bash

basepath=$(cd `dirname $0`; pwd)
cd $basepath

# 从 appname 文件读取服务名称
if [ -f "appname" ]; then
    name=$(cat appname)
else
    echo "当前目录下未找到 appname 文件，请确保该文件存在。"
    exit 1
fi

project=$basepath/..
cd $project

dest=dest

if [ -d $dest ]; then
	rm -fr $dest
fi
mkdir -p $dest

addVersion() {
  GIT_COMMIT_SHA1=$(git describe --always --abbrev=10 --dirty --tags)
  GIT_COMMIT_DATE=$(git log -1 --format=%cd --date=iso)
  GIT_COMMIT_SUBJECT=$(git log -1 --format=%s)
  GIT_BRANCH_NAME=$(git rev-parse --abbrev-ref HEAD)

  echo ""
  echo "============================== GIT COMMIT LOG =============================="
  echo "=== git commit SHA1     :    ${GIT_COMMIT_SHA1}"
  echo "=== git branch name     :    ${GIT_BRANCH_NAME}"
  echo "=== git commit date     :    ${GIT_COMMIT_DATE}"
  echo "=== git commit subject  :    ${GIT_COMMIT_SUBJECT}"
  echo "============================================================================"
  echo ""
}
addVersion

packageName=gold-md-backtest/biz/config
LDFLAGS="-X '$packageName.GitCommitSha1=$GIT_COMMIT_SHA1' \
         -X '$packageName.GitCommitDate=$GIT_COMMIT_DATE' \
         -X '$packageName.GitCommitSubject=$GIT_COMMIT_SUBJECT' \
         -X '$packageName.GitBranchName=$GIT_BRANCH_NAME'"

GOARCH=amd64 go build -ldflags "$LDFLAGS" -o $dest/$name

cp -fr conf $dest
cp -fr scripts/* $dest