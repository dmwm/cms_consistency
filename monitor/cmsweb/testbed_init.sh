cd $HOME
wget https://cernbox.cern.ch/index.php/s/Fnxcj4x3sUm92cs/download -O config.cmsweb-k8s-services-testbed
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=$PWD/config.cmsweb-k8s-services-testbed
