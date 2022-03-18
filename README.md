# gsr-order-book
Orderbook proof of concept


Requirements: Java8 Junit4 Maven GitHub

Features implemented: 3 CCY pairs. Basic logic according to specification.

Bonus features: Lock free multi-threading

The orderbook framework is built in the following way:

Serializer -- Some sort of serializer responsible for getting messages off the wire. As a dummy I've added a String parser, but in reality this would ideally be something clever to avoid creating millions of objects.

Distributor -- The distributors only responsibility is to direct messages to the appropriate receiver queue. I have split the processing between order book sides. That is, there is one processing thread per book side. This should cater for scalablity (More currencies can be supported by just adding more cores).

Book Side Processor -- The book side processor handles all incoming request (market data updates as well as analytics requests) which is relevant to its book. And responds with nothing (for MD updates) and analytics responses (correspondingly).

Benefits of this solution: Thread communication is entirely lock free and all synchronization is handled in compare-and-swap fashion. This should allow the enginge to perform without long delays for handling critical sections, context switches and so forth. Ideally the book threads would be pinned to particular processor cores on which nothing else would be scheduled by the OS. There are some drawbacks to this -- Both analytics requests and market data updates must be handled within the same thread. But this is a managable trade off, which I believe in most scenarios far outweigh the gain of having to synchronize and context switch.

Howto run: Either run the OrderBookIntegrationTest or play with the OrderBookReplicatorRunner which has a main method (but for the rest pretty useless).

I've used standard Maven and the OrderBookIntegrationTest loads market data updates from a resource based csv file. Don't compile this in a Jar and try running it -- it won't work.
