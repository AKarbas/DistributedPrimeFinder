# DistributedPrimeFinder
A distributed system for finding (large) primes using [Sieve of Eratosthenes](https://en.wikipedia.org/wiki/Sieve_of_Eratosthenes), Written in Scala as the [second assignment](https://drive.google.com/file/d/1qixsoHjyPoRjgdbQAkH4vUx6jggKmCah/view) of [DE-2019](https://sites.google.com/view/dataengineering-2019/home) Course in [Sharif University of Technology](https://www.sharif.edu/).

## Design:
The system is comprised of a Master and multiple Slaves.
The work is done as follows:

* The range of (1, ∞) is split into chunks of length n, and each chunk is assigned to a worker which in turn marks all of its non-primes using previous chunks containing prime numbers up to sqrt(M) (M being the largest number in the assigned chunk.)
* The worker requests said previous chunks one by one from the master.
* Later on, The worker packages it's package of prime numbers and sends it to master for storage.


## Possible Future Work:
* Currently it is assumed that the master has infinite (lol i mean large) and fault-tolerant storage for the numbers. this can be managed through the use of a distributed storage system like [Apache Hadoop HDFS](https://hadoop.apache.org/) or [Ceph](https://ceph.com) etc., And
if such a system is used, the workers can directly request/put chunks from/to the storage system.
* Another thing about the current system is the master being a Single Point of Failure; Which can be alleviated through the use of a Hot-Standby or Multi-Master design.
* If a distributed system for storage is used, the Master can be entirely replaced by [Apache Zookeper](https://zookeeper.apache.org), since then the only job of the master would be coordination and the use of Zookeeper can make the system Master-Less/Homogenous.