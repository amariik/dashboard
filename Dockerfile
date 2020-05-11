FROM oraclelinux:7-slim as builder

# It is important that these ARG's are defined after the FROM statement
ARG ACCESS_TOKEN_USR="nothing"
ARG ACCESS_TOKEN_PWD="nothing"

ARG release=19
ARG update=6

RUN  yum -y install oracle-release-el7 && yum-config-manager --enable ol7_oracle_instantclient && \
     yum -y install oracle-instantclient${release}.${update}-basic oracle-instantclient${release}.${update}-devel oracle-instantclient${release}.${update}-sqlplus

RUN yum -y install git
RUN yum -y install oracle-golang-release-el7
RUN yum -y install golang

RUN rm -rf /var/cache/yum
	
# Create a netrc file using the credentials specified using --build-arg
RUN printf "machine github.com\n\
    login ${ACCESS_TOKEN_USR}\n\
    password ${ACCESS_TOKEN_PWD}\n\
    \n\
    machine api.github.com\n\
    login ${ACCESS_TOKEN_USR}\n\
    password ${ACCESS_TOKEN_PWD}\n"\
    >> /root/.netrc
RUN chmod 600 /root/.netrc

# Set the working directory outside $GOPATH to enable the support for modules.
WORKDIR /src

# Fetch dependencies first; they are less susceptible to change on every build
# and will therefore be cached for speeding up the next build
RUN git clone https://github.com/amariik/dashboard.git

WORKDIR /src/dashboard
RUN go mod download

# Build the executable to `/app`. Mark the build as statically linked.
RUN go build -o godash

FROM oraclelinux:7-slim as final

ARG release=19
ARG update=6

RUN  yum -y install oracle-release-el7 && yum-config-manager --enable ol7_oracle_instantclient && \
     yum -y install oracle-instantclient${release}.${update}-basic

WORKDIR /src/dashboard

COPY ./cfg ./cfg
COPY --from=builder /src/dashboard/godash /src/dashboard/godash

EXPOSE 9999

ENTRYPOINT ./godash
