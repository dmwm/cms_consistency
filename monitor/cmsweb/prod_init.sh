cd ~

wget https://cernbox.cern.ch/index.php/s/M4UtMwA2IxJhvx6/download -O config.cmsweb-k8s-services-prodnew
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=$PWD/config.cmsweb-k8s-services-prodnew
cd -



#wget https://cernbox.cern.ch/index.php/s/GRiyu10zwWHlOKc/download -O config.cmsweb-k8s-services-prod
#export OS_TOKEN=$(openstack token issue -c id -f value)
#export KUBECONFIG=$PWD/config.cmsweb-k8s-services-prod
