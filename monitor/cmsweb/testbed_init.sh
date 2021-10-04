cd $HOME
wget https://cernbox.cern.ch/index.php/s/o4pP0BKhNdbPhCv/download -O config.cmsweb-testbed
export OS_TOKEN=$(openstack token issue -c id -f value)
export KUBECONFIG=$PWD/config.cmsweb-testbed
cd -
