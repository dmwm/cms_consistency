#!/bin/sh

cd ~rucio/cms_consistency
git pull				# make sure to pick up the latest version

cd site_cmp3

./site_cmp3.sh $@

