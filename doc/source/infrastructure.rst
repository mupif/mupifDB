Infrastructure
#################

MuPIF-DB is one (crucial) piece in a whole infrastructure which is described in this section.

The infrastructure communicates via Wireguard VPN with star topology. Central nodes in the star are running on CTU premises as containers. Each central node provides the following services to the rest of the network:


The following table summarizes services offered by the central node of the VPN:

.. csv-table::
   :header-rows: 1

   port,protocol,service
   22,ssh,(administrative access)
   5000,http,REST API
   5555,http,MupifDB web interface
   8000,http,VPN monitor
   8001,http,Scheduler monitor
   10000,Pyro,Pyro nameserver

Wireguard VPN
==============


Database
---------

Database access is hidden behind unified REST API, and there are several database supported (MongoDB, Granta SQL).
