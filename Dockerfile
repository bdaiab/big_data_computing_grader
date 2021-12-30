FROM korekontrol/ubuntu-java-python3

RUN apt update && \
  apt install -y git wget nano && \
  pip install --upgrade pip && \
  ln -s /usr/bin/python3 /usr/bin/python && \
  rm -rf /var/lib/apt/lists/*


WORKDIR /edx/app/xqueue_watcher
COPY requirements /edx/app/xqueue_watcher/requirements
COPY . /edx/app/xqueue_watcher
RUN pip install -r requirements/production.txt && \
	pip install -r requirements.txt
	
RUN wget "https://mirrors.tuna.tsinghua.edu.cn/apache/spark/spark-3.0.3/spark-3.0.3-bin-hadoop2.7.tgz" \
    && tar -xf spark-* -C /edx/app/xqueue_watcher \
    && rm -rf spark-3.0.3-bin-hadoop2.7.tgz \
	&& wget "https://repos.spark-packages.org/graphframes/graphframes/0.8.2-spark3.0-s_2.12/graphframes-0.8.2-spark3.0-s_2.12.jar" \
	&& mv graphframes-0.8.2-spark3.0-s_2.12.jar /edx/app/xqueue_watcher/spark-3.0.3-bin-hadoop2.7/jars \
	&& mv log4j.properties /edx/app/xqueue_watcher/spark-3.0.3-bin-hadoop2.7/conf

ENV SPARK_HOME=/edx/app/xqueue_watcher/spark-3.0.3-bin-hadoop2.7
EXPOSE 6066 8080 7077 4044 18080

RUN useradd -m --shell /bin/false sandbox \
	&& chown -R sandbox /edx/app/xqueue_watcher/checkpoint \
	&& chmod 755 /edx/app/xqueue_watcher/checkpoint \
	&& chown -R sandbox /edx/app/xqueue_watcher/log \
	&& chmod 755 /edx/app/xqueue_watcher/log
USER sandbox

CMD /edx/app/xqueue_watcher/spark-3.0.3-bin-hadoop2.7/sbin/start-master.sh && \
	/edx/app/xqueue_watcher/spark-3.0.3-bin-hadoop2.7/sbin/start-slave.sh spark://`hostname`:7077 && \
	python -m xqueue_watcher -d ./