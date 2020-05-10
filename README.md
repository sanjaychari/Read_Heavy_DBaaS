# CC_Project
# Steps to run the project :
1. Create three instances. Add the repository in all three instances. Set up AWS target groups and an application load balancer for the first two instances.
2. Install docker and docker-compose on all three instances. You can follow https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=94798094.
3. In the first instance, run $cd Assignment3/rides, and then $sudo docker-compose up --build
4. In the second instance, run $cd Assignment3/users, and then $sudo docker-compose up --build
5. In the third instance, run $cd PROJECT, and then $sudo docker-compose up --build
