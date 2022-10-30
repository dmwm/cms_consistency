if [ ! -f `pwd`/config.cmsweb-k8s-services-prod ]; then
	wget  https://cernbox.cern.ch/remote.php/dav/public-files/cg373hUAglJ2mwI/config.cmsweb-k8s-services-prod -O `pwd`/config.cmsweb-k8s-services-prod
fi
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=`pwd`/config.cmsweb-k8s-services-prod
alias kc='kubectl -n ruciocm'
alias kc
