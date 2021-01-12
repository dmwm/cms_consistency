cd ~
wget https://cernbox.cern.ch/index.php/s/GRiyu10zwWHlOKc/download -O config.cmsweb-k8s-services-prod
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=$PWD/config.cmsweb-k8s-services-prod
cd -
