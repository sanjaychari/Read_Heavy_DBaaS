# CC_Project

# About The Project 
The project deals with servicing a read heavy application using Flask, Docker, RabbitMQ, and ZooKeeper. There are two entities in the database of the project : users and rides. We programmed various RESTful API endpoints to access and modify the information related to these two entities.
Our infrastructure consists of an AWS application load balancer, that forwards incoming requests according to the path of the request to the respective target group. We have two target groups : the users instance and the rides instance. The API endpoints on these target groups send database read/write requests to the database orchestrator instance. RabbitMQ, Docker SDK are used for scale in/scale out in the Docker instance; and Zookeeper is used for high availability.

# Design
Our infrastructure consists of an AWS application load balancer, that forwards incoming requests according to the path of the request to the respective target group. All requests with path /api/v1/users are forwarded to the users target group; and all the other requests are forwarded to the rides instance. We have two target groups : the users instance and the rides instance. The API endpoints on these target groups send database read/write requests to the database orchestrator instance.
In the database orchestrator instance, worker containers take care of read/write to the database. Since we are servicing a read heavy application, it is assumed that a single master container for write to the database is sufficient, and slave containers are slave containers are scaled in/out depending on the number of incoming read requests to the database. To achieve this infrastructure, we utilize RabbitMQ with the following layout.
       
The database orchestrator consists of 5 containers running initially : RabbitMQ, Zookeeper, master container, slave container, and the orchestrator container.

We used pika to connect to our RabbitMQ container from the orchestrator container.We followed the official examples on the RabbitMQ website to achieve the above design. For scale in/out, we have a manager thread that runs for the entire duration of the orchestrator life cycle. In this manager thread, we utilise Docker SDK to create/kill containers according to the number of incoming read requests in the past 2 minutes.

For high availability using Zookeeper, we made use of a manager thread; along with the watch APIs that are built in to Kazoo. We connect to the Zookeeper container from the orchestrator container using the Kazoo client. We ensure two paths "/workers/slaves" and "/workers/master"; to which children are added and deleted according to the list of containers at that point of time. In the manager thread, creation and deletion of znodes is taken care of; according to the container list. If a container is killed at any point of time apart from auto scaling, the Zookeeper manager thread detects it as a crash.

• If the master crashes, the slave with the lowest pid is elected as master; and a new slave is created. To implement the transition from slave to master, we utilised two methods : change of environment variable, and container rename. When a slave is elected to master, we changed the value of the IS_MASTER environment variable to True and renamed it to <slavename>-MASTER. In the worker code, we check for the name of the worker container; and run the master code if MASTER is present in its name.
• If a slave crashes, a new slave is created.

# Steps to run the project 
1. Create three instances. Add the repository in all three instances. Set up AWS target groups and an application load balancer for the first two instances.
2. Install docker and docker-compose on all three instances. You can follow https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=94798094.
3. In the first instance, run $cd Assignment3/rides, and then $sudo docker-compose up --build
4. In the second instance, run $cd Assignment3/users, and then $sudo docker-compose up --build
5. In the third instance, run $cd PROJECT, and then $sudo docker-compose up --build
