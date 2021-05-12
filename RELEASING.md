# Releasing

## Update and Tag Catheter

(note, you do not need to build/deploy, it will be done as part of the nephron build)
```
cd catheter
git checkout master
git pull
mvn versions:set -DnewVersion=1.0.2
git commit -a -m 'release: Catheter v1.0.2'
git tag -u opennms@opennms.org -s v1.0.2
cd ..
```

## Update and Tag Nephron

```
mvn versions:set -DnewVersion=0.2.2
# edit pom to use released Catheter version
vim generator/pom.xml main/pom.xml
git commit -a -m 'release: Nephron v0.2.2'
git tag -u opennms@opennms.org -s v0.2.2
git push origin v0.2.2
```

## Release Nephron

```
# the "release" profile turns on GPG signing
mvn -Prelease clean deploy
```

Because of the way the submodules work, you will need to release Catheter to Maven Central manually.

1. go to [the sonatype repo interface](https://oss.sonatype.org/#stagingRepositories)
2. select the staging repository that contains catheter
3. "close" it
4. "release" it

## Update Nephron and Catheter versions

```
cd catheter
mvn versions:set -DnewVersion=1.0.3-SNAPSHOT
git commit -a -m '1.0.2 -> 1.0.3-SNAPSHOT'
# catheter submodule is checked out as read-only by default, set the repo explicitly when pushing
git push git@github.com:opennms-forge/catheter.git
git push --tags git@github.com:opennms-forge/catheter.git
cd ..

mvn versions:set -DnewVersion=0.2.3-SNAPSHOT
# edit pom to use snapshot Catheter version
vim generator/pom.xml main/pom.xml
git commit -a -m '0.2.2 -> 0.2.3-SNAPSHOT'
git push
git push --tags
```

