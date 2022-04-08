# Modify configs for SSL connections in JDK 8

## PROBLEM DESCRIPTION
MySQL/Redis in cloud services maybe configed as SSL only, which prevent Java server accessing without SSL configuration. In addition to configuration in Java Server Process, we have to check whether SSL is enabled in JDK.

## SOLUTION
Edit `jdk.tls.disabledAlgorithms` configuration in `java.security`(Path maybe `/etc/alternatives/jre_1.8.0_openjdk/lib/security/java.security` in Centos 7), remove `SSL` if exists.

##### Before
![Before](./pics/ssl_config_before.png)

##### After
![After](./pics/ssl_config_after.png)

## REFERENCE

1. [JDK8版本过高引起MySQL连接失败](https://juejin.cn/post/6969142310718144520)