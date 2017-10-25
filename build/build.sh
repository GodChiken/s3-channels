#!/bin/bash
set -e

START_MINIO="docker run -d -p 9000:9000 -e \"MINIO_ACCESS_KEY=$ACCESS_KEY\" -e \"MINIO_SECRET_KEY=$SECRET_KEY\" minio/minio server /data"

if [[ $TRAVIS_PULL_REQUEST == "false" ]]
then
	openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in $BUILD_DIR/pubring.gpg.enc -out $BUILD_DIR/pubring.gpg -d
	openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in $BUILD_DIR/secring.gpg.enc -out $BUILD_DIR/secring.gpg -d
	openssl aes-256-cbc -pass pass:$ENCRYPTION_PASSWORD -in $BUILD_DIR/deploy_key.pem.enc -out $BUILD_DIR/deploy_key.pem -d

	if [ -e "release.version" ] && [ $TRAVIS_BRANCH == "master" ]
	then
		echo "Performing a release..."
		RELEASE_VERSION=$(cat release.version)

		eval "$(ssh-agent -s)"
		chmod 600 $BUILD_DIR/deploy_key.pem
		ssh-add $BUILD_DIR/deploy_key.pem
		git config --global user.name "CI"
		git config --global user.email "ci@mentegy.github.io"
		git config --global push.default matching
		git remote set-url origin git@github.com:mentegy/s3-channels.git
		git fetch --unshallow
		git checkout master || git checkout -b master
		git reset --hard origin/master

		git rm release.version
		git commit -m "[release] remove release.version"
		git push

		mvn -B clean release:prepare --settings build/settings.xml -DreleaseVersion=$RELEASE_VERSION
		mvn release:perform --settings build/settings.xml
	elif [[ $TRAVIS_BRANCH == "master" ]]
	then
		echo "Publishing a snapshot..."
		eval $START_MINIO
		mvn clean org.jacoco:jacoco-maven-plugin:prepare-agent package deploy --settings build/settings.xml

	else
		echo "Publishing a branch snapshot..."
		mvn clean versions:set -DnewVersion=$TRAVIS_BRANCH-SNAPSHOT
		eval $START_MINIO
		mvn org.jacoco:jacoco-maven-plugin:prepare-agent package deploy --settings build/settings.xml
	fi
else
	echo "Running build..."
	eval $START_MINIO
	mvn clean org.jacoco:jacoco-maven-plugin:prepare-agent package
fi