#!/bin/sh

rm -rf out
npm run docs
cd out
git init .
git remote add origin git@github.com:Runnable/ponos.git
git checkout -b gh-pages
git add .
git commit -m "update docs"
git push -f origin gh-pages
cd ..
rm -rf out
