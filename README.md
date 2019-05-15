#### Aim

Add feature: add distributed trasaction.  

```
         A                       MQ                         B
  trasaction begin 
  send preapred message          =>        push message   
  sql
  trasaction commit
  send commited message          =>        push message   => 
```

-----

#### orignal project   
[**nsq**](https://github.com/nsqio/nsq) is a realtime distributed messaging platform designed to operate at scale, handling
billions of messages per day.


#### remarks  
[protocol](http://nsq.io/clients/tcp_protocol_spec.html)    
[installing](http://nsq.io/deployment/installing.html)  
[docker_deployment](http://nsq.io/deployment/docker.html)  
[features_guarantees](http://nsq.io/overview/features_and_guarantees.html)  
[contributors](https://github.com/nsqio/nsq/graphs/contributors)  
[client_libraries](http://nsq.io/clients/client_libraries.html)  

