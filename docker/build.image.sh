#!/bin/bash
MESSAGE="Bye! ✋\n"
#
# Make sure docker is available
DOCKER=$(which docker)
if [[ "$DOCKER" == "" ]]
then
  echo -e "Docker not available on this machine, exiting."
  echo -e "To install Docker, see https://store.docker.com/search?type=edition&offering=community"
  exit 1
else
  echo -e "+-------------------------------+"
  echo -e "| Building a Spark Docker image |"
  echo -e "+-------------------------------+"
fi
echo -en "Ready ? > "
read REPLY
if [[ ! ${REPLY} =~ ^(yes|y|Y)$ ]]
  then
  echo "Canceled."
  exit 0
fi
#
DOCKER_FILE=spark-debian.Dockerfile
IMAGE_NAME=oliv-spark
RUN_CMD="docker run -it --rm -e USER=root -p 8080:8080 $IMAGE_NAME:latest /bin/bash"
#
MESSAGE="---------------------------------------------------\n"
MESSAGE="${MESSAGE}Log in using: docker run -it --rm -e USER=root -p 8080:8080 $IMAGE_NAME:latest /bin/bash\n"
MESSAGE="${MESSAGE}---------------------------------------------------\n"
#
docker build -f ${DOCKER_FILE} -t ${IMAGE_NAME} .
#
# Now run
echo -e "To create a container, run $RUN_CMD..."
printf "%b" "$MESSAGE"
