# Apache ManifoldCF Confluence Connector

Atlassian Confluence repository and authority connector for Apache ManifoldCF

## Building this connector
```
svn co http://svn.apache.org/repos/asf/manifoldcf/trunk/ manifoldcf
cd manifoldcf
mvn clean install -DskipTests -Dmaven.test.skip
cd -
git clone https://github.com/adperezmorales/confluence-manifold-connector.git
cd confluence-manifold-connector
mvn clean install -DskipTests -Dmaven.test.skip
```

## Deploy and configure the Confluence connector

TBD

## Configure Manifold and start!

TBD