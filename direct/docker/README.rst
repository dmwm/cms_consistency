Building Docker image
---------------------

0. git pull <cms_consistency git URL>

1. cd cms_consistency/direct

2. cp config.json-sample ./docker/config.json

3. cd ./docker

4. vi config.json
    edit Oracle access parameters (host, port, user, schema, etc.)
    edit LFN-to-PFN conversion rules, default and/or specific for specific RSE(s)
    
5. docker build -t cms-replicas-direct

6. docker run --rm -ti cms-replicas-direct /bin/bash

7. Inside the docker container:
    $ cd cms_consistency/direct
    $ python replicas_for_rse.py -c /tmp/config.json <RSE name>
    
