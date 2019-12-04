#!/bin/sh
# template for username/password for sonatype repository server
cp settings.xml ~/.m2/settings.xml

# ## generate gpg
# gpg --full-generate-key
# ## send to gpg server
# gpg --keyserver pool.sks-keyservers.net --send-key 2E5260D120241F6F8E35370D293C0...
# ## import signing key
# ## this is how to export them
# gpg --export-secret-keys 'Markus Cozowicz (com.microsoft.accumulo) <marcozo@microsoft.com>' | base64 -w 0
echo $ossrh_gpg | base64 -d | gpg --import -

MAVEN_OPTS="verify gpg:sign deploy:deploy -Dmaven.test.skip=true -DskipITs -Dossrh.username=$ossrh_username -Dossrh.password=$ossrh_password"

# to use the snapshot from oss.sonatype.org 
# * add http://oss.sonatype.org/content/repositories/snapshots 
# * reference com.microsoft.accumulo:accumulo-spark-connector:1.0.0
# * reference com.microsoft.accumulo:accumulo-spark-datasource:1.0.0
# 
# more details at https://stackoverflow.com/questions/7715321/how-to-download-snapshot-version-from-maven-snapshot-repository

# For a proper release:
# * remove -SNAPSHOT in pom.xml
# * visit https://oss.sonatype.org/#stagingRepositories and "close & release" the staged .jar
# * see https://oss.sonatype.org/#stagingRepositories
mvn -f ../pom.xml install
mvn -f ../datasource/pom.xml $MAVEN_OPTS -DshadedArtifactAttached=false
mvn -f ../iterator/pom.xml $MAVEN_OPTS -DshadedArtifactAttached=false