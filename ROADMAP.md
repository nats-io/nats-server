# Roadmap

This is the roadmap for the NATS Server. This document discusses the high level goals and future plans 
for the NATS Server, and where appropriate, the relationship to dependent projects such as 
[NATS Streaming](https://github.com/nats-io/nats-streaming-server) and the 
[NATS Connector Framework](https://github.com/nats-io/nats-connector-framework).  

Our project plan is kept updated on [the wiki](https://github.com/nats-io/gnatsd/wiki).

* [NATS Goals](#nats-goals)
* [NATS Components](#nats-components)
* [Project Planning](#project-planning)

This roadmap is a living document, providing an overview of the goals and
considerations made in respect of the future of the project.

## NATS Goals

The overaching goal of NATS is to provide a messaging platform that prioritizes the following key characteristics:
  
 * Performance - achieve the highest message throughput and lowest latency possible
 * Stability - "always on". Nothing we put in NATS should cause it to crash, and NATS should guard itself against unruly client behavior that might compromise performance or availability for all clients.
 * Simplicity - a compact, simple, and easily mastered API that requires no knowledge about the implementation of the broker (`gnatsd`), and a broker that is lightweight, requiring minimal configuration, system resources and external dependencies.
 * Security - NATS supports basic security features: authentication, authorization and encryption (TLS) 

## NATS Components

NATS consists of the NATS Server (`gnatsd`) and the supported client libraries, which are listed 
[here](http://nats.io/download).

Components of the NATS Project are managed via GitHub [milestones](https://github.com/nats-io/gnatsd/milestones).
Upcoming features and bugfixes for a component will be added to the relevant milestone. 

If a feature or bugfix is not part of a milestone, it is currently unscheduled for implementation. 

### NATS Server

[ Under Construction ]

### NATS Client

[ Under Construction ]

## Project Planning

An [Open-Source Planning Process](https://github.com/nats-io/gnatsd/wiki/Open-Source-Planning-Process) is 
used to define the Roadmap. [Project Pages](https://github.com/nats-io/gnatsd/wiki) define the 
goals for each Milestone and identify current progress.

The NATS Master Project Plan can be found [here](https://github.com/nats-io/roadmap/wiki).


