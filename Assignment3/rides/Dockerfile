FROM python:3.6
COPY . /rides
WORKDIR /rides/api
ENV TEAM_NAME=CC_0272_0278_0710_1910
RUN pip3 install --upgrade pip
RUN pip3.6 install -r ./requirements.txt
ENTRYPOINT ["python"]
CMD ["apis_rides.py"]
