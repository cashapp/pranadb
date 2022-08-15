# Guide for Local Deployment

## TLS Certs Generation

The local deployment includes the TLS certs for client and server and the CA cert used to sign both certificates. This 
is for testing purposes only.

To regenerate all the certificates do:

```shell
cd ${PROJECT_DIR}/local-deployment
./generate-tls-certs.sh
```
 