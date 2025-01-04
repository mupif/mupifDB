# Infrastructure

MuPIF-DB is one (crucial) piece in a whole infrastructure which is described in this section.

The infrastructure communicates via Wireguard VPN with star topology. Central nodes in the star are running on CTU premises (`mech` server) as containers. Each central node provides a number of services to the internal network (within the VPN) and some publicly accessible services (with OAuth access) to the public internet. One of the VPN's is called `test` (which will be used as placeholder in the documentation). Each VPN is configured in a directory on the `mech` server, e.g. `/root/mupif-networks/test` (called VPN-home in the following)

## Central node container

Each central node is containerized (Docker) and can be re-started by issuing `make restartonly` in VPN-home. Look into the Makefile for other shorthands. Entering the running container is done via `make join`.

The container is configured via variables in `VPN-home/container.yml` which defines variables such as `MUPIF_NS_HOST`, `MUPIF_NS_PORT`, `MUPIF_BRANCH`, `MUPIFDB_BRANCH` and others (see [Dockerfile](https://github.com/mupif/infrastructure-docker/blob/main/Dockerfile)), as well as ports which are open for the outside world. Historically, they were several, but later, only two ports whould be exposed:

1. Wireguard port (for clients to connect to the VPN), accessible from outside;
2. HTTP reverse proxy port, accessible from localhost only, which is proxied to https://test.mupif.org (for `test`);

Directories are mounted such that `/var/lib/mupif` is home of the `mupif` user, and persistent data are in `/var/lib/mupif/persistent`, which is mapped from `VPN-home/persistent` on mech. Thus it is easy to access live logs from `mech` without being logged into the container. This can be done conveniently as e.g.:

    cd /root/mupif-networks/test/persistent/logs
    xtail -f . | ccze -A



### Web server

The webserver is a virtual host, e.g. `test.mupif.org` (see `/etc/apache2/sites-enabled/mupif-vpns.conf` for details) and the SSL is handled via letencrypt; because letsencrypt cannot read apache's macros (which are used to define mupif VPNs), it is configured in `/etc/letsencrypt/renewal/test.mupif.org.conf` using the `webroot` authenticator for letsencrypt. The `/var/www/mupif-vpns/test` directory is the webroot, though the useful content is proxied from the container under `mon/`, `webapi/` etc. paths.

### Backups

MongoDB (database) is backed up daily from within the container, as mech's `/mnt/bacup/mupif/test/mongodb` is accessible as `/mnt/backup` from within the container, which runs cron and does a daily dump of the databse.

## Wireguard VPN

VPN-home contains `test-vpn.json` file describing the VPN topology:


    {
        "iface":"test",
        "addr":"172.24.1.1/24",
        "keepalive":20,
        "extAddr":"147.32.140.158",
        "extPort":"51823",
        "hubDir":"/root/mupif-networks/test/persistent/",
        "peerDir":"/root/mupif-networks/test/vpn-peers",
        "iptables":0,
        "restart":0,
        "peerMap":"/root/mupif-networks/test/persistent/peers-test.json",
        "peers":[
                ["mon","172.24.1.2"],
                ["client01","172.24.1.10"],
                …

    }

Running `make keys` will create new client's keys (existing clients will be left untouched, so this can be done repeatedly without invalidating other's access) which will appear in `vpn-peers` directory. The config file can be imported on client's machine (when running Linux and NetworkManager) via

    sudo nmcli connection import type wireguard test_client01.conf

. When a new client is added, wireguard on the central node must re-read the list of clients. To do this, enter the container from `mech` via `make join` and run `wg-quick down /var/lib/mupif/persistent/test.conf` followed by `supervisorctl restart wireguard` (which will bring the interface up again). Or, if there is no danger in service disruption, restarting the container itself will make the new client known to the VPN as well.


## Services on the central node

The services are defined in `/etc/supervisor/conf.d/*` service files; those files are copied when the docker container is built.

### VPN

The wireguard central node is accessible from the outside world at mech.fsv.cvut.cz:* (port number varies by the container; see `container.yml` for the respctive container.

### Outer HTTP services

All HTTP-based services are reverse-proxied by apache inside the container, exposed to the host and accessed through `https://$MUPIF_VPN_NAME.mupif.org/`.

#### Safe REST API

FastAPI-based "safe" (restricted) REST API runs on port 8006, but is accessed through `https://$MUPIF_VPN_NAME.mupif.org/safe-api/` (port 8006 is reverse-proxied by apache inside the container).

#### Web interface

Flask-based web interface runs on port 5555, is reverse-proxied with apache inside of the containers, and then accessed through `https://$MUPIF_VPN_NAME.mupif.org/web/`. It requires authentication to Google to perform any operations.

#### Monitor

The Quasar-based monitor is served statically using the apache inside the container (which otherwise acts as reverse proxy for other services) and is reachable as `https://$MUPIF_VPN_NAME.mupif.org/mon/`.

### Inner services

These services are only accessible from within the VPN.

#### Nameserver

Pyro4 nameserver runs on the central node at port `10000`. Containers define `MUPIF_NS` (which is automatically picked up by MuPIF) via Dockerfile. Its data are stored in `nameserver.sqlite` in the persistent directory (`$MUPIF_PERSIST_DIR`).

#### REST API

FastAPI-based server runs on port 8005. It is usually accessed using functions in the `mupifDB.api.client` module.

#### ssh

`ssh` server permits remote login inside the container as `root`. It is only allowed to login via key-based authentication, and the `/root/.ssh/authorized_keys` is copied into the container when the container is built.

### Background services

### MongoDB

The database is never accessed directly by clients, it is only used through the REST API. It runs on the default MongoDB port 27017 and only permits connections from localhost (unauthenticated).

### Scheduler

Scheduler is periodically checking for jobs submitted to the database; it interacts with the database, and is an independent background process which is normally not interacted with.
