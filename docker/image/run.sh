#!/bin/sh

cd ~/cms_consistency
git pull

cd site_cmp3

./site_cmp3.sh $@

