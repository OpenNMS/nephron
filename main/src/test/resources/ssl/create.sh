#!/bin/bash
echo "123456" > secret

openssl req -new -x509 -days 9999 -keyout ca.key -out ca.crt -subj "/C=DE/L=Fulda/CN=Certificate Authority" -passout pass:1234
openssl x509 -text -noout -in ca.crt
keytool -genkey -keystore server.keystore -alias localhost -dname CN=localhost -keyalg RSA -validity 365 -ext san=dns:localhost -storepass 123456 -keypass 123456
keytool -list -v -keystore server.keystore -storepass 123456

keytool -certreq -keystore server.keystore -alias localhost -file server.unsigned.crt -storepass 123456

openssl x509 -req -CA ca.crt -CAkey ca.key -in server.unsigned.crt -out server.crt -days 9999 -CAcreateserial -passin pass:1234
keytool -import -file ca.crt -keystore server.keystore -alias ca -storepass 123456 -noprompt

keytool -import -file server.crt -keystore server.keystore -alias localhost -storepass 123456 -noprompt
keytool -list -v -keystore server.keystore -storepass 123456
keytool -import -file ca.crt -keystore server.truststore -alias ca -storepass 123456 -noprompt
keytool -list -v -keystore server.truststore -storepass 123456

# client

keytool -import -file ca.crt -keystore client.truststore -alias ca -storepass 123456 -noprompt
keytool -list -v -keystore client.truststore -storepass 123456

keytool -genkey -keystore client.keystore -alias client -dname CN=client -keyalg RSA -validity 9999 -storepass 123456 -keypass 123456
keytool -certreq -keystore client.keystore -alias client -file client.unsigned.crt -storepass 123456
openssl x509 -req -CA ca.crt -CAkey ca.key -in client.unsigned.crt -out client.crt -days 365 -CAcreateserial -passin pass:1234
keytool -import -file ca.crt -keystore client.keystore -alias ca -storepass 123456 -noprompt
keytool -import -file client.crt -keystore client.keystore -alias client -storepass 123456 -noprompt
