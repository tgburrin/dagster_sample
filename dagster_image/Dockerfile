from python:3.13-trixie

RUN mkdir -p /root/.dbt

COPY requirements.txt /tmp/requirements.txt

RUN apt-get update \
	&& apt-get install -y \
		netcat-traditional \
		iputils-ping \
		bash \
		vim \
		postgresql \
		libsasl2-dev \
		libsasl2-modules-gssapi-mit \
		strace \
		less \
		ca-certificates-java \
		python3-pip \
		libleveldb-dev \
		jq

RUN sed -i 's/" let g:skip_defaults_vim = 1/let g:skip_defaults_vim = 1/g' /etc/vim/vimrc

RUN pip install -r /tmp/requirements.txt
