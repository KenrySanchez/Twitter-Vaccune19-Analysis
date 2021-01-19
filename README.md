# Twitter-Vaccune19-Analysis

Twitter Analysis to understand people opinions about covid-19 vaccune.

## Twitter API Connection

1. At the Root folder, there is a `twitter4j.properties` file with the credentials.

- `oauth.consumerKey`: Twitter API key.
- `oauth.consumerSecret`: Twitter API key secret.
- `oauth.accessToken`: Twitter Access token.
- `oauth.accessTokenSecret`: Twitter Access token secret.

We can change these credentials in order to `connect with other Twitter API Account`.

## Standalone Deployment Instructions

1. Have Maven installed `version > 1.8`.
2. Run `mvn clean install`.
3. Run the following command.

```
 mvn exec:java -Dexec.mainClass="com.alecspopa.storm.Topology"
```