# EventBus (Distributed Message Bus)
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
**EventBus=RPC+MQ, a reliable distributed message bus which is build base on open sourcing message midleware. EventBus offers not only traditional feature of MQ system (event notification, multicast, broadcast etc.) but also synchronize call and high availability such as application multi active, service nearby, dark launch. The enhancement of fault tolerant enable EventBus running more reliable and offer an all days online service.**

## Architecture
<div align=center>

</div>

EventBus includes the following components:
* **Broker**: Offering message store via the mechanism of Topic and queue. Broker registers to NameServer periodically. Brokers in the same cluster should register to the same NameServer to keep route info in all name server is consistent.

* **NameServer**: NameServer maintains route info of Topic and provide clients an interface to get route info of given Topic.

## Definition of Service & Topic
&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;
Each service in EventBus correspond to exactly a Topic. Topic is named according to the unique ID of the service and the deployment area of the service. Each service needs a unique identity, which can be represented by a digital ID or string. Each deployment area is represented by a string of length 3, which is consist of numbers and letters.

Topic is named in the following format:
```
[Codigo de area] - [ID unico de servicio]
``` 
For example, the service ID of the balance query service is 20210001, and it is deployed in the area of "A10". The Topic of such service in the area of A10 is named "A10-20190001".


### Setting up your development environment
Quick start, read[ this ](docs/en/quickstart.md) to get more information.   
Examples are also provide in EventBus-examples module, get more detail from [here](eventbus-examples).

