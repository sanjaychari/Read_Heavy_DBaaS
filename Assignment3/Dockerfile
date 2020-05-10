FROM python:3.6
COPY . /users
WORKDIR /users/api
ENV TEAM_NAME=CC_0272_0278_0710_1910 
RUN pip3 install --upgrade pip
RUN pip3.6 install -r ./requirements.txt
ENTRYPOINT ["python"]
CMD ["apis_users.py"]
