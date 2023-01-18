if [ ! -f `pwd`/config.cmsweb-testbednew ]; then
	wget  https://cernbox.cern.ch/remote.php/dav/public-files/ibTvZrd5Sm6MlyD/config.cmsweb-testbednew -O `pwd`/config.cmsweb-testbednew
fi
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=`pwd`/config.cmsweb-testbednew
alias kc='kubectl -n ruciocm'
alias kc
