#FROM centos/python-38-centos7 
FROM conda/miniconda3-centos7

RUN conda update -n base -c defaults conda

RUN pip install webpie pythreader jinja2

WORKDIR /root

RUN echo 2

ADD start.sh ./start.sh
RUN mkdir app
ADD app ./app

EXPOSE 8080

ENTRYPOINT ["/root/start.sh"]




